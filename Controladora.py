
import sqlalchemy as _sqla
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import Sequence
from sqlalchemy.schema import Table
from sqlalchemy import inspect
import sqlalchemy.ext.automap as _sqla_automap
import sqlalchemy.ext.declarative as _sqla_decl
from frozendict import frozendict
import json as _json
from functools import partial as _partial, reduce as _reduce
from collections import deque as _deque
from operator import add as _add

import urllib
from SPARQLWrapper import SPARQLWrapper, JSON
from past.builtins import basestring
import rdflib as _rdf
from rdflib import RDFS
from rdflib.namespace import FOAF
from rdflib.store import Store
from sqlalchemy.orm import sessionmaker , relationship
import sqlalchemy.orm as _sqla_orm
import tiposComunes as _tp
import directMapping as _dm
import os 
class Controladora(_rdf.store.Store):

    """
    """
    
    def __init__(self, configuration=None, id=None, base_iri=None, rdb_metadata=None,
                 orm_classes=None, orm=None):

        self._id = id
        self._base_iri = base_iri if base_iri is not None else id

        self._namespaces = {}
        self._prefix_by_namespace = {}

        self._rdb = configuration
        self._rdb_metadata = rdb_metadata if rdb_metadata is not None else MetaData()
        self._rdb_transaction = None

        Session = sessionmaker(bind = self._rdb)
        self.session = Session()

        self._orm = orm
        self.OrmBase = declarative_base()
        self._orm_classes = orm_classes
        self._orm_mappers = None
        self._orm_columns_properties = None
        self._orm_columns_rdf_datatypes = None
        self._orm_relationships = None
        self._orm_bnode_tables = None

        if configuration:
            self.open(configuration)

    
    def connect_to_database( self,  configuration ):
        #leer la configuracion del archivo y colocarla en los argumentos para el sqlachemy
        #por el momento voy a llenar a mano la configuracion

        self._rdb = create_engine('postgresql://Wendy:qwerty@localhost:5432/postgres')
        #self._rdb = create_engine('sqlite:///D:\\school.db', echo=False)
        self._rdb.connect()

        self.OrmBase = declarative_base()
        #creando una sesion
        Session = sessionmaker(bind = self._rdb)
        self.session = Session()
        self.insp = inspect( self._rdb )
        self.AutomapBase = _sqla_automap.automap_base()
        self.AutomapBase.prepare(self._rdb, reflect=True)

    def _rdb_from_configuration(self, configuration):
    
        if isinstance(configuration, _sqla.engine.interfaces.Connectable):
            return configuration
 
        elif isinstance(configuration, basestring):
            try:
                parts = _json.loads(configuration)
            except TypeError as exc:
                raise TypeError('invalid configuration type {!r}: {}'
                                 .format(type(configuration), exc))
            except ValueError as exc:
                raise ValueError('invalid configuration {!r}: {}'
                                  .format(configuration, exc))

            if len(parts) > 2:
                raise ValueError('invalid configuration {!r}: expecting a JSON'
                                  ' sequence of two or less items'
                                  .format(configuration))

            if parts:
                rdb_args = parts[0]
            else:
                rdb_args = ()

            if len(parts) == 2:
                rdb_kwargs = parts[1]
            else:
                rdb_kwargs = {}

        else:
            try:
                #rdb_args, rdb_kwargs = configuration
                a=0
            except TypeError as exc:
                raise TypeError('invalid configuration type {!r}: {}'
                                 .format(type(configuration), exc))
            except ValueError as exc:
                raise ValueError('invalid configuration {!r}: {}'
                                  .format(configuration, exc))

        #return _sqla.create_engine(*rdb_args, **rdb_kwargs)
        #return _sqla.create_engine('sqlite:///school.db', echo= False)
        engine_str='postgresql://' + os.getlogin() + '@localhost:5432/DBInput'
        return _sqla.create_engine(engine_str)
        #return _sqla.create_engine('postgresql://Wendy:qwerty@localhost:5432/DBVialidad')

    def _table_type_triples(self, table_iri):
        table_orm_mapper = self._orm_mappers[table_iri]
        subject_pkey_cols = table_orm_mapper.primary_key
        subject_node_from_sql = self._row_node_from_sql_func(table_iri)
        query = self._orm.query(*subject_pkey_cols)
        for subject_pkey_values in query.all():
            yield (subject_node_from_sql(zip(subject_pkey_cols,
                                           subject_pkey_values)),
                       _rdf.RDF.type, table_iri)

    def create(self, configuration):
        rdb = self._rdb_from_configuration(configuration)
        self.OrmBase.metadata.create_all(bind=rdb, checkfirst=True)

    def open(self, configuration, create=False, reflect=True):

        self._rdb = self._rdb_from_configuration(configuration)

        if create and self._rdb_metadata:
            self.create(self._rdb)

        if self._orm_classes is None:
            #se encarga de establecer las conexiones de sqlalchemy con el engine que cree en _rdb_from_configuration
            self.OrmBase = _dm.orm_automap_base(name='OrmBase',
                                                base_iri=self._base_iri,
                                                bind=self._rdb,
                                                metadata=self._rdb_metadata)
        self.OrmBase.prepare(engine= self._rdb, reflect=reflect)
        self._rdb_metadata = self.OrmBase.metadata
        self._orm_classes = \
                frozendict((self._table_iri(class_.__table__.name), class_)
                            for class_ in self.OrmBase.classes)
        Session = sessionmaker(bind = self._rdb)
        self.session = Session()
        
        if self._orm_mappers is None:
            mappers_items = []
            colprops_items = []
            cols_datatypes_items = []
            rels_items = []
            bnode_tables = set()

            for table_iri, class_ in self._orm_classes.items():
                class_mapper = _sqla.inspect(class_)
                props = _orm_column_property_by_name(mapper=class_mapper)
                mappers_items.append((table_iri, class_mapper))
                colprops_items.append((table_iri, props))
                cols_datatypes_items\
                 .append((table_iri,
                          {colname: _tp.tipo_de_datos_rdf_comun_al_tipo_sql
                                     (prop.columns[0].type)
                           for colname, prop in props.items()}))
                rels_items\
                 .append((table_iri,
                          _orm_relationship_by_local_column_names
                           (mapper=class_mapper)))
                if class_mapper.has_pseudo_primary_key:
                    bnode_tables.add(table_iri)
            self._orm_mappers = frozendict(mappers_items)
            self._orm_columns_properties = frozendict(colprops_items)
            self._orm_columns_rdf_datatypes = frozendict(cols_datatypes_items)
            self._orm_relationships = frozendict(rels_items)
            self._orm_bnode_tables = frozenset(bnode_tables)

        if self._orm is None:
            self._orm = _sqla_orm.sessionmaker(bind=self._rdb)()
        self._rdb_transaction = self._rdb.begin().transaction

    def _table_iri(self, tablename):
        return self._prefixed_iri(_tp.iri_safe(tablename))

    def _prefixed_iri(self, rel_iri):
        if self._base_iri is not None:
            return _rdf.URIRef(u'{}{}'.format(self._base_iri, rel_iri))
        return _rdf.URIRef(rel_iri)


    def _row_bnode_from_sql(self, table_iri, pkey_items):
        return _rdf.BNode(self._row_str_from_sql(table_iri, pkey_items))
        
    def _row_iri_from_sql(self, table_iri, pkey_items):
        return _rdf.URIRef(self._row_str_from_sql(table_iri, pkey_items))

    def _row_node_from_sql(self, table_iri, pkey_items):
        return self._row_node_from_sql_func(table_iri)(pkey_items)

    def _row_node_from_sql_func(self, table_iri):
        #en orm_bnode_tables estan las tablas que no tenian primary_key y se les dio una
        if table_iri in self._orm_bnode_tables:
            return _partial(self._row_bnode_from_sql, table_iri)
        else:
            return _partial(self._row_iri_from_sql, table_iri)

    def _row_str_from_sql(self, table_iri, pkey_items):
        return u'{}/{}'\
                .format(table_iri,
                        ';'.join(u'{}={}'
                                  .format(_tp.iri_safe(col.name),
                                          _tp.iri_safe
                                           (_tp.rdf_literal_from_sql
                                             (value, sql_type=col.type)))
                                 for col, value in pkey_items))

    def _parse_row_node(self, node):
        try:
            table_iri_str, _, pkeyspec = node.rpartition('/')
        except AttributeError:
            raise TypeError(u'invalid row node {!r}: not a string'
                             .format(node))

        if not table_iri_str or '=' not in pkeyspec:
            raise ValueError\
                   (u'invalid row node {!r}: does not match format {!r}'
                     .format(node, 'table/colname=value[;colname=value]...'))

        table_iri = _rdf.URIRef(table_iri_str)

        if table_iri in self._orm_bnode_tables:
            if not isinstance(node, _rdf.BNode):
                raise ValueError('invalid node type {!r} for blank node table'
                                  ' {!r}: not blank node'
                                  .format(node.__class__, table_iri))
        else:
            if not isinstance(node, _rdf.URIRef):
                raise ValueError('invalid node type {!r} for IRI node table'
                                  ' {!r}: not IRI node'
                                  .format(node.__class__, table_iri))

        pkey = {}
        cols_props = self._orm_columns_properties[table_iri]
        cols_datatypes = self._orm_columns_rdf_datatypes[table_iri]
        for name_irisafe, value_irisafe in (item.split('=')
                                            for item in pkeyspec.split(';')):
            colname = urllib.parse.unquote(name_irisafe)
            value_rdf = _rdf.Literal(urllib.parse.unquote(value_irisafe),
                                     datatype=cols_datatypes[colname])
            pkey[cols_props[colname].class_attribute] = \
                _tp.sql_literal_from_rdf(value_rdf)

        return table_iri, pkey

    def _subject_triples(self, subject_node, predicate_pattern,
                         object_pattern):

        try:
            subject_table_iri, subject_pkey = \
                self._parse_row_node(subject_node)
        except (TypeError, ValueError):
            return
        subject_class = self._orm_classes[subject_table_iri]
        subject_cols_props = \
            self._orm_columns_properties[subject_table_iri]

        query = self._orm.query(subject_class)\
                         .filter(*(attr == value
                                   for attr, value
                                   in subject_pkey.items()))

        if predicate_pattern is None:
            if object_pattern is None:
                # IRI, *, *

                subject_mapper = self._orm_mappers[subject_table_iri]
                subject_cols = subject_mapper.columns
                subject_cols_props = \
                    self._orm_columns_properties[subject_table_iri]
                subject_rels = \
                    self._orm_relationships[subject_table_iri].values()

                query = query.with_entities()

                for predicate_col in subject_cols:
                    predicate_colname = predicate_col.name
                    predicate_iri = \
                        self._literal_property_iri(subject_table_iri,
                                                   predicate_colname)
                    predicate_prop = subject_cols_props[predicate_colname]
                    predicate_attr = predicate_prop.class_attribute

                    query = query.add_columns(predicate_attr)

                for predicate_prop in subject_rels:
                    object_table = predicate_prop.target
                    object_table_iri = self._table_iri(object_table.name)
                    object_cols_props = \
                        self._orm_columns_properties[object_table_iri]
                    object_pkey_attrs = \
                        [object_cols_props[col.name].class_attribute
                         for col
                         in object_table.primary_key.columns]

                    query = query.outerjoin(predicate_prop.class_attribute)\
                                 .add_columns(*object_pkey_attrs)

                query_result_values = query.first()
                query_result_values_pending = _deque(query_result_values)
                subject_cols_values = \
                    [query_result_values_pending.popleft()
                     for _ in range(len(subject_cols))]

                yield (subject_node, _rdf.RDF.type, subject_table_iri)

                for predicate_col, object_value \
                        in zip(subject_cols, subject_cols_values):

                    if object_value is None:
                        continue

                    predicate_iri = \
                        self._literal_property_iri(subject_table_iri,
                                                   predicate_col.name)

                    yield (subject_node,
                           predicate_iri,
                           _tp.rdf_literal_from_sql
                            (object_value,
                             sql_type=predicate_col.type))

                for predicate_prop in subject_rels:
                    object_table = predicate_prop.target
                    object_pkey_cols = object_table.primary_key.columns
                    object_pkey_values = \
                        [query_result_values_pending.popleft()
                         for _ in range(len(object_pkey_cols))]
                    object_node_from_sql = \
                        self._row_node_from_sql_func\
                         (self._table_iri(object_table.name))

                    if any(value is None for value in object_pkey_values):
                        continue

                    predicate_iri = \
                        self._ref_property_iri\
                         (subject_table_iri,
                          (col.name
                           for col in predicate_prop.local_columns))

                    yield (subject_node,
                           predicate_iri,
                           object_node_from_sql(zip(object_pkey_cols,
                                                    object_pkey_values)))

            elif isinstance(object_pattern, _rdf.Literal):
                # IRI, *, literal

                subject_mapper = self._orm_mappers[subject_table_iri]
                subject_cols_props = \
                    self._orm_columns_properties[subject_table_iri]
                object_sql_types = \
                    _tp.sql_literal_types_from_rdf\
                     (object_pattern.datatype)

                for predicate_col in subject_mapper.columns:
                    predicate_sql_type = predicate_col.type
                    if isinstance(predicate_sql_type, object_sql_types):
                        predicate_colname = predicate_col.name
                        predicate_prop = \
                            subject_cols_props[predicate_colname]
                        predicate_attr = predicate_prop.class_attribute
                        object_sql_literal = \
                            _tp.sql_literal_from_rdf(object_pattern)
                        query_cand = \
                            query.filter(predicate_attr
                                          == object_sql_literal)

                        if self._orm.query(query_cand.exists()).scalar():
                            predicate_iri = \
                                self._literal_property_iri\
                                 (subject_table_iri, predicate_colname)
                            yield (subject_node, predicate_iri, object_pattern)

            elif isinstance(object_pattern, _rdf.URIRef):
                # IRI, *, IRI

                if object_pattern == subject_table_iri:
                    if self._orm.query(query.exists()).scalar():
                        yield (subject_node, _rdf.RDF.type, subject_table_iri)
                    return

                try:
                    object_table_iri, object_pkey = \
                        self._parse_row_node(object_pattern)
                except (TypeError, ValueError):
                    return

                subject_rels = self._orm_relationships[subject_table_iri]
                object_cols_props = \
                    self._orm_columns_properties[object_table_iri]

                for predicate_prop in subject_rels.values():
                    query_cand = \
                        query.join(predicate_prop.class_attribute)\
                             .filter(*(attr == value
                                       for attr, value
                                       in object_pkey.items()))
                    if self._orm.query(query_cand.exists()).scalar():
                        predicate_iri = \
                            self._ref_property_iri\
                             (subject_table_iri,
                              (col.name
                               for col in predicate_prop.local_columns))
                        yield (subject_node, predicate_iri, object_pattern)

            else:
                return

        elif predicate_pattern == _rdf.RDF.type:
            if object_pattern is None \
                   or (isinstance(object_pattern, _rdf.URIRef)
                       and object_pattern == subject_table_iri):
                if self._orm.query(query.exists()).scalar():
                    yield (subject_node, _rdf.RDF.type, subject_table_iri)

        elif isinstance(predicate_pattern, _rdf.URIRef):
            try:
                predicate_attr = \
                    self._predicate_orm_attr(predicate_pattern)
            except ValueError:
                return
            predicate_prop = predicate_attr.property

            if isinstance(predicate_prop, _sqla_orm.RelationshipProperty):
                if object_pattern is None:
                    # IRI, ref IRI, *

                    object_table = predicate_prop.target
                    object_table_iri = self._table_iri(object_table.name)
                    object_pkey_cols = object_table.primary_key.columns

                    query = \
                        query.join(predicate_attr)\
                             .with_entities(*object_pkey_cols)
                    for object_pkey_values in query.all():
                        yield (subject_node,
                               predicate_pattern,
                               self._row_iri_from_sql(object_table_iri,
                                                      zip(object_pkey_cols,
                                                          object_pkey_values)))

                elif isinstance(object_pattern, _rdf.URIRef):
                    # IRI, ref IRI, IRI

                    try:
                        object_table_iri, object_pkey = \
                            self._parse_row_node(object_pattern)
                    except (TypeError, ValueError):
                        return

                    object_cols_props = \
                        self._orm_columns_properties[object_table_iri]

                    query = query.join(predicate_attr)\
                                 .filter(*(attr == value
                                           for attr, value
                                           in object_pkey.items()))

                    if self._orm.query(query.exists()).scalar():
                        yield (subject_node, predicate_pattern, object_pattern)
                    else:
                        return

                else:
                    return

            else:
                predicate_col, = predicate_attr.property.columns

                if object_pattern is None:
                    # IRI, non-ref IRI, *
                    query = query.with_entities(predicate_attr)\
                                 .filter(predicate_attr != None)
                    for value, in query.all():
                
                        yield (subject_node, predicate_pattern,
                               _tp.rdf_literal_from_sql
                                (value, sql_type=predicate_col.type))

                elif isinstance(object_pattern, _rdf.Literal):
                    # IRI, non-ref IRI, literal

                    if object_pattern.datatype \
                           not in _tp.rdf_datatypes_from_sql\
                                   (predicate_col.type):
                        return

                    object_sql_literal = \
                        _tp.sql_literal_from_rdf(object_pattern)
                    query = \
                        query.filter(predicate_attr != None,
                                     predicate_attr == object_sql_literal)

                    if self._orm.query(query.exists()).scalar():
                        yield (subject_node, predicate_pattern, object_pattern)

                else:
                    return

        else:
            return

    def triples(self, subject_pattern, predicate_pattern, object_pattern,
                context=None):
        if context is not None \
               and not (isinstance(context, _rdf.Graph)
                        and isinstance(context.identifier, _rdf.BNode)):
            return
        if subject_pattern is None:
            if predicate_pattern is None:
                for subject_table_iri in self._orm_classes.keys():
                    for triple \
                            in self._table_allpredicates_triples\
                                (subject_table_iri, object_pattern):
                        #cambiar por un yield
                        yield( triple, None)
                        #una tripla de un individuo contiene su primary_key, type, iri de la tabla
                        #ademas, por cada propiedad de la tabla se produce una nueva tipla
                        #que contiene primary_key o id del indiv, nombre de la propiedad, valor y tipo segun rdflib
                        #y finalmente una tripla por cada relacion que se establezca de ese individuo con otro individuo de otra tabla
            elif predicate_pattern == _rdf.RDF.type:
                if object_pattern is None:
                    for subject_table_iri in self._orm_classes.keys():
                        for triple \
                                in self._table_type_triples(subject_table_iri):
                            yield triple, None
                elif isinstance(object_pattern, _rdf.URIRef):
                    for triple in self._table_type_triples(object_pattern):
                        yield triple, None
                else:
                    return

            elif isinstance(predicate_pattern, _rdf.URIRef):
                try:
                    predicate_attr = \
                        self._predicate_orm_attr(predicate_pattern)
                except ValueError:
                    return
                predicate_prop = predicate_attr.property
                subject_table_iri = \
                    self._table_iri(predicate_prop.parent.mapped_table)

                for triple in self._table_predicate_triples(subject_table_iri,
                                                            predicate_pattern,
                                                            object_pattern):
                    yield triple, None

            else:
                return

        elif isinstance(subject_pattern, (_rdf.URIRef, _rdf.BNode)):
            for triple in self._subject_triples(subject_pattern,
                                                predicate_pattern,
                                                object_pattern):
                yield triple, None

        else:
            return
       

    def _predicate_orm_attr(self, iri):
    
        try:
            table_iri_str, _, colspec = iri.partition('#')
        except AttributeError:
            raise TypeError(u'invalid predicate IRI {!r}: not a string'
                             .format(iri))

        if not table_iri_str or not colspec:
            raise ValueError(u'invalid predicate IRI {!r}:'
                              u' does not match either format {!r}'
                              .format(iri,
                                      ('table#colname',
                                       'table#ref-colname[;colname]...')))

        table_iri = _rdf.URIRef(table_iri_str)

        if colspec.startswith('ref-'):
            cols = frozenset(urllib.parse.unquote(colname)
                             for colname in colspec[4:].split(';'))
            try:
                prop = self._orm_relationships[table_iri][cols]
            except KeyError:
                raise ValueError('unknown reference property {!r}'.format(iri))

        else:
            col = urllib.parse.unquote(colspec)
            try:
                prop = self._orm_columns_properties[table_iri][col]
            except KeyError:
                raise ValueError('unknown literal property {!r}'.format(iri))

        return prop.class_attribute


    def _table_predicate_triples(self, table_iri, predicate_iri,object_pattern):

        subject_mapper = self._orm_mappers[table_iri]
        subject_pkey_cols = subject_mapper.primary_key
        subject_pkey_len = len(subject_pkey_cols)
        subject_node_from_sql = self._row_node_from_sql_func(table_iri)
        try:
            predicate_attr = self._predicate_orm_attr(predicate_iri)
        except ValueError:
            return
        predicate_prop = predicate_attr.property

        query = self._orm.query(*subject_pkey_cols)

        if isinstance(predicate_prop, _sqla_orm.RelationshipProperty):
            if object_pattern is None:
                # *, ref IRI, *

                object_table = predicate_prop.target
                object_pkey_cols = object_table.primary_key.columns
                object_node_from_sql = \
                    self._row_node_from_sql_func\
                     (self._table_iri(object_table.name))

                query = query.join(predicate_attr)\
                             .add_columns(*object_pkey_cols)

                for result_values in query.all():
                    subject_pkey_values = result_values[:subject_pkey_len]
                    object_pkey_values = result_values[subject_pkey_len:]
                    yield (subject_node_from_sql(zip(subject_pkey_cols,
                                                     subject_pkey_values)),
                           predicate_iri,
                           object_node_from_sql(zip(object_pkey_cols,
                                                    object_pkey_values)))

            elif isinstance(object_pattern, (_rdf.URIRef, _rdf.BNode)):
                # *, ref IRI, node

                try:
                    object_table_iri, object_pkey = \
                        self._parse_row_node(object_pattern)
                except (TypeError, ValueError):
                    return

                query = query.join(predicate_attr)\
                             .filter(*(attr == value
                                       for attr, value
                                       in object_pkey.items()))

                for subject_pkey_values in query.all():
                    yield (subject_node_from_sql(zip(subject_pkey_cols,
                                                     subject_pkey_values)),
                           predicate_iri,
                           object_pattern)

            else:
                return

        else:
            predicate_col, = predicate_attr.property.columns
            object_sql_type = predicate_col.type

            query = query.add_columns(predicate_attr)\
                         .filter(predicate_attr != None)

            if isinstance(object_pattern, _rdf.Literal):
                # *(IRI), non-ref IRI, literal

                if object_pattern.datatype \
                       not in _tp.rdf_datatypes_from_sql(object_sql_type):
                    return

                object_sql_literal = \
                    _tp.sql_literal_from_rdf(object_pattern)
                query = query.filter(predicate_attr == object_sql_literal)

            if object_pattern is None \
                 or isinstance(object_pattern, _rdf.Literal):
                # *(IRI), non-ref IRI, *
                query = query.add_columns(predicate_attr)
                for result_values in query.all():
                    yield (subject_node_from_sql
                            (zip(subject_pkey_cols,
                                 result_values[:subject_pkey_len])),
                           predicate_iri,
                           _tp.rdf_literal_from_sql
                            (result_values[-1], sql_type=predicate_col.type))

            else:
                return
   
    def _table_allpredicates_triples(self, table_iri, object_pattern):
        subject_mapper = self._orm_mappers[table_iri]
        subject_pkey_cols = subject_mapper.primary_key
        subject_node_from_sql = self._row_node_from_sql_func(table_iri)
        query = self._orm.query(*subject_pkey_cols)
       
        if object_pattern is None:
            # *(IRI), *, *

            subject_mapper = self._orm_mappers[table_iri]
            subject_cols = subject_mapper.columns
            subject_cols_props = self._orm_columns_properties[table_iri]
            subject_rels = self._orm_relationships[table_iri].values()
            query = query.with_entities()
            for predicate_col in subject_cols:
                predicate_prop = subject_cols_props[predicate_col.name]
                predicate_attr = predicate_prop.class_attribute
                query = query.add_columns(predicate_attr)

            for predicate_prop in subject_rels:
                object_table = predicate_prop.target
                object_table_iri = self._table_iri(object_table.name)
                object_cols_props = \
                    self._orm_columns_properties[object_table_iri]
                predicate_attr = predicate_prop.class_attribute
                if not table_iri == object_table_iri:
                    query = \
                        query.outerjoin(predicate_attr)\
                             .add_columns(*(object_cols_props[col.name]
                                             .class_attribute
                                            for col
                                            in object_table.primary_key.columns))
            #en query esta por cada tabla el select de cada una de sus propiedades con un left outer join con las llaves
            #foraneas que se obtienen en las relaciones inter tablas
            for query_result_values in query.all():
                query_result_values_pending = _deque(query_result_values)
                subject_cols_values = [query_result_values_pending.popleft()
                                       for _ in range(len(subject_cols))]
                subject_pkey_values = (subject_cols_values[i]
                                       for i, col in enumerate(subject_cols)
                                       if col in subject_pkey_cols)
                subject_node = subject_node_from_sql(zip(subject_pkey_cols,
                                                         subject_pkey_values))
                yield (subject_node, _rdf.RDF.type, table_iri)

                for predicate_col, object_value in zip(subject_cols,
                                                       subject_cols_values):

                    if object_value is None:
                        continue

                    predicate_iri = \
                        self._literal_property_iri(table_iri,
                                                   predicate_col.name)
                    #por cada tabla devuelve el valor de cada una de sus propiedades para cada individuo
                    #estudiantes/id=1 estudiantes#id 1
                    #estudiantes/id=1 estudiantes#name Daniel
                    #estudiantes/id=1 estudiantes#apellidos de la osa fernandez
                    #estudiantes/id=1 estudiantes#escuela_id 1
                   
                    yield (subject_node, predicate_iri,
                           _tp.rdf_literal_from_sql
                            (object_value, sql_type=predicate_col.type))
                    yield(predicate_iri, RDFS.domain, table_iri                 )
                    for a in _tp.rdf_datatypes_from_sql(sql_type=predicate_col.type):
                        if a is not None:
                            yield(predicate_iri, RDFS.range, a)
                for predicate_prop in subject_rels:
                    object_table = predicate_prop.target
                    object_pkey_cols = object_table.primary_key.columns
                    object_pkey_values = \
                        [query_result_values_pending.popleft()
                         for _ in range(len(object_pkey_cols))]

                    if any(value is None for value in object_pkey_values):
                        continue

                    predicate_iri = \
                        self._ref_property_iri\
                         (table_iri,
                          (col.name for col in predicate_prop.local_columns))
                    #por cada relacion que se establece entre dos tablas voy a devolver que individuo de la primera tabla
                    #se relaciona con que individuo de la segunda tabla
                    #ejemplo estudiantes/id=1 estudiantes#ref-escuela_id Escuela/id=1
                    yield (subject_node,
                           predicate_iri,
                           self._row_node_from_sql\
                            (self._table_iri(object_table.name),
                             zip(object_pkey_cols, object_pkey_values)))
                    yield(predicate_iri, RDFS.domain, table_iri                 )
                    yield(predicate_iri, RDFS.range,  self._table_iri(object_table.name))
        elif isinstance(object_pattern, _rdf.Literal):
            # *(IRI), *, literal
            subject_cols_props = \
                self._orm_columns_properties[table_iri]
            object_sql_types = \
                _tp.sql_literal_types_from_rdf(object_pattern.datatype)

            for predicate_col in subject_mapper.columns:
                predicate_sql_type = predicate_col.type
                if isinstance(predicate_sql_type, object_sql_types):
                    
                    predicate_colname = predicate_col.name
                    predicate_iri = \
                        self._literal_property_iri(table_iri,
                                                   predicate_colname)
                    predicate_prop = subject_cols_props[predicate_colname]
                    predicate_attr = predicate_prop.class_attribute
                    object_sql_literal = \
                        _tp.sql_literal_from_rdf(object_pattern)
                    query_cand = \
                        query.filter(predicate_attr == object_sql_literal)
                    for subject_pkey_values in query_cand.all():
                       yield (subject_node_from_sql(zip(subject_pkey_cols,
                                                         subject_pkey_values)),
                               predicate_iri, object_pattern)

        elif isinstance(object_pattern, (_rdf.URIRef, _rdf.BNode)):
            # *(IRI), *, IRI

            if object_pattern == table_iri:
                for subject_pkey_values in query.all():
                    yield (subject_node_from_sql(zip(subject_pkey_cols,
                                                     subject_pkey_values)),
                           _rdf.RDF.type, table_iri)
                return

            try:
                object_table_iri, object_pkey = \
                    self._parse_row_node(object_pattern)
            except (TypeError, ValueError):
                return

            subject_rels = self._orm_relationships[table_iri]
            object_cols_props = self._orm_columns_properties[object_table_iri]

            for predicate_prop in subject_rels.values():
                predicate_iri = \
                    self._ref_property_iri(table_iri,
                                           (col.name
                                            for col
                                            in predicate_prop.local_columns))

                query_cand = \
                    query.join(predicate_prop.class_attribute)\
                         .filter(*(attr == value
                                   for attr, value in object_pkey.items()))
                for subject_pkey_values in query_cand.all():
                    yield (subject_node_from_sql(zip(subject_pkey_cols,
                                                     subject_pkey_values)),
                           predicate_iri, object_pattern)

        else:
            return

    def _literal_property_iri(self, table_iri, colname):
        return _rdf.URIRef(u'{}#{}'.format(table_iri,
                                    _tp.iri_safe(colname)))

    def _ref_property_iri(self, table_iri, fkey_colnames):
        return _rdf.URIRef(u'{}#ref-{}'
                            .format(table_iri,
                                    ';'.join(_tp.iri_safe(colname)
                                             for colname
                                             in fkey_colnames)))

cont= Controladora()

def _orm_column_property_by_name(mapper):
    return frozendict((prop.key, prop) for prop in mapper.column_attrs)

def _orm_relationship_by_local_column_names(mapper):
    return frozendict((frozenset(col.name for col in rel.local_columns),
                        rel)
                       for rel in mapper.relationships
                       if not rel.collection_class)

def _print_graph_matplotlib(g):
    import networkx as nx
    import matplotlib.pyplot as plt

    #url = 'https://www.w3.org/TeamSubmission/turtle/tests/test-30.ttl'

    #g = _rdf.Graph()
    #result = g.parse(url, format='turtle')
    G = nx.DiGraph()
    for u,v, k in g:
        G.add_edge(u , k ,title= v.split('#')[1])
        
    # Plot Networkx instance of RDF Graph
    pos = nx.spring_layout(G, scale=2)
    edge_labels = nx.get_edge_attributes(G, 'title')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color= 'b')
    options= { 'node_color': 'blue',    'node_size': 200,   'width': 3, 'scale':2 , 'edge_labels' : edge_labels,
    'label_pos': 0.5, 'font_size' : 10, 'font_color': 'k', 'font_family' : 'sans-serif' ,  'font_weight': 'normal',
     'alpha': 1.0, 'bbox': None, 'ax' : None, 'rotate' : True,    }

    nx.draw(G, pos, with_labels=True, **options)
    plt.show()

def _print_graph_rdf2dot(g):
    import io
    import  pydotplus
    from IPython.display import display, Image
    from rdflib.tools.rdf2dot import rdf2dot

    def visualize(g):
        stream = io.StringIO()
        rdf2dot(g, stream, opts = {display})
        dg = pydotplus.graph_from_dot_data(stream.getvalue())
        png = dg.create_png()
        display(Image(png))
    visualize(g)

def mostrar_identificadores():
    """
    Devuelve una tripla con los nombres de las tablas en la primera posicion,
    las propiedadades con su tabla a la que corresponden,
    las relaciones con las dos tablas a las que referencian
    """
    a=0
    cont = Controladora(base_iri="http://www.example.org/")
    cont.open(configuration= a, create=False, reflect=True)
    nombres_de_las_tablas=[]
    dic_propiedades_por_nombre_de_tabla = []
    dic_relaciones_por_nombre_de_tablas=[]
    for table_iri, class_ in cont._orm_classes.items():
        class_mapper = _sqla.inspect(class_)
        nombres_de_las_tablas.append(class_.__table__.name)
        dic_propiedades_por_nombre_de_tabla.append(frozenset((class_.__table__.name ,prop.key) for prop in class_mapper.column_attrs))
        for rel in class_mapper.relationships:
            if not rel.collection_class:
                dic_relaciones_por_nombre_de_tablas.append(frozenset((class_.__table__.name, col.name, rel.target.name) for col in rel.local_columns))
    return nombres_de_las_tablas, dic_propiedades_por_nombre_de_tabla, dic_relaciones_por_nombre_de_tablas


def run_code(_DATOS_EDITADOS):
    cont = Controladora(base_iri="http://www.example.org/")
    #cont.create(None)
    n = _rdf.Namespace("http://www.example.org#")

    store=_rdf.plugin.get("SQLAlchemy", Store )(identifier=_rdf.URIRef("rdflib_test"))
    uri = _rdf.Literal("sqlite://")
    g = _rdf.Graph(store ,identifier=_rdf.URIRef("rdflib_test_graph"))
    g.open(uri, create=True)
    
    g.bind(n, 'example', False)
    a=0
    cont.open(configuration= a, create=False, reflect=True)
    #Poblar_a_mano(cont)
    #print(_DATOS_EDITADOS)
    conjunto=[]
    for triple , context in cont.triples(subject_pattern = None,predicate_pattern = None,object_pattern = None, context= None):
        if (triple in conjunto): pass#print(triple) 
        else :
            conjunto.append(triple)
            a,b,c =triple
            edit_ref=b
            if b.__contains__('ref-'):
                edit_ref= b.split('ref-')[1]
            else:
                edit_ref=b.split('#')[1]
            new_triple=triple
            if a in _DATOS_EDITADOS:
                new_triple= (_DATOS_EDITADOS[a], new_triple[1],new_triple[2])
            if b in _DATOS_EDITADOS or edit_ref in _DATOS_EDITADOS:
                new_triple=(new_triple[0], _rdf.URIRef(new_triple[0]+'#' + str(_DATOS_EDITADOS[edit_ref]).replace(' ','_')), new_triple[2])
            if c in _DATOS_EDITADOS:
                new_triple=(new_triple[0],new_triple[1],_DATOS_EDITADOS[c])
            #print(new_triple)
            g.add(new_triple)

    g.serialize(destination= 'output.rdf', format="pretty-xml")
    fp = open('outfile.rdf','wb')
    fp.write(g.serialize(format='xml'))
    fp.close()

    

    return g
    #_print_graph_matplotlib(g)
    #_print_graph_rdf2dot(g)



#asi se crea una tabla
class User(cont.OrmBase):
    __tablename__= 'usuarios'

    id= Column(_sqla.Integer,Sequence('user_id_seq'), primary_key=True)
    name = Column(_sqla.String)
    fullname= Column(_sqla.String)
    nickname= Column(_sqla.String)
    
    #esta es la representacion, no es obligatoria pero es como se imprime
    def __repr__(self):
        return "<User(name= '%s', fullname= '%s', nickname= '%s' )>" % (self.name,self.fullname, self.nickname)

class Estudiante(cont.OrmBase):
    __tablename__ = 'estudiantes'

    id= Column(_sqla.Integer, primary_key=True)
    name = Column(_sqla.String)
    escuela_id=Column(ForeignKey('Escuela.id'))
    escuela= relationship("Escuela", back_populates="estudiantes")
    
    __mapper_arg__= { 'polymorphic_identity': 'estudiante', 'polymorphic_on': type}
    def __repr__(self):
        return "<Estudiante(nombre= '%s' , escuela ='%s' )>" % (self.name, self.escuela)

class Escuela(cont.OrmBase):
    __tablename__= 'Escuela'

    id = Column(_sqla.Integer, primary_key=True)
    nombre_escuela = Column(_sqla.String)
    estudiantes = relationship("Estudiante", back_populates="escuela")

def Poblar_a_mano(cont):
    
    ass=Escuela(nombre_escuela='UH-Matcom')
    asd= Escuela(nombre_escuela='UH-FBio')
    #cont.session.add()
    dany=Estudiante(name='Daniel de la osa fernandez')
    dany.escuela=ass
    cont.session.add(dany)
    
    dany=Estudiante(name='Dayrene Fundora Gonzalez')
    dany.escuela=ass
    cont.session.add(dany)
    #cont.session.commit()
    
    dany=Estudiante(name='Wendy Diaz Ramirez')
    dany.escuela=ass
    cont.session.add(dany)
    #cont.session.commit()
    
    dany=Estudiante(name='Irania Ramirez Alfonso')
    dany.escuela=asd
    cont.session.add(dany)
    #cont.session.commit()

    dany=Estudiante(name='Fermin Lazaro Felipe Tame')
    dany.escuela=asd
    cont.session.add(dany)
    cont.session.commit()

    our_students= cont.session.query(Estudiante).all()
    for our_student in our_students:
       print(our_student)
    

def Consulta(grafo, query):
    print(query)
    #if not query:
    #query = "SELECT * WHERE { ?s ?p ?o} "
    return grafo.query(query)
    #sparql= SPARQLWrapper("http://example.org/sparql")
    #sparql.setQuery(query)
    #ret=sparql.query()
    #return ret
#    except:
#        print("El texto no es una consulta SPARQL valida.")
    
#result = g.parse("http://localhost:3030/$/stats/TestTesis")
#print("graph has %s statements." % len(g))




