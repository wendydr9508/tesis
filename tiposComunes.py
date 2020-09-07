from binascii import hexlify as _bytes2hexstr, unhexlify as _hexstr2bytes
from urllib.parse import quote as _pct_encoded
from datetime import timedelta as _timedelta
import rdflib as _rdf
import sqlalchemy as _sqla

#diccionario con la traduccion de tipos SQL a RDF
_TIPOS_COMUNES_RDF_AL_TIPO_SQL = \
    {_sqla.sql.sqltypes._Binary: _rdf.XSD.hexBinary,
     _sqla.Boolean: _rdf.XSD.boolean,
     _sqla.Date: _rdf.XSD.date,
     _sqla.DateTime: _rdf.XSD.dateTime,
     _sqla.Float: _rdf.XSD.double,
     _sqla. Integer: _rdf.XSD.integer,
     _sqla.Interval: _rdf.XSD.duration,
     _sqla.Numeric: _rdf.XSD.decimal,
     _sqla.String: _rdf.XSD.string,
     _sqla.Time: _rdf.XSD.time,
     _sqla.sql.type_api.TypeEngine: None,
     }

def tipo_de_datos_rdf_comun_al_tipo_sql(sql_type):
    #ejemplo puede entrar INTEGER
    if not isinstance(sql_type, type):
        sql_type = sql_type.__class__
    #lo cual no es un tipo puro, por tanto me quedo con el __class__
    #voy a llamar a otro metodo que se encarge de indexar en el diccionario de tipos comunes
    return _tipo_de_datos_rdf_comun_al_tipo_sql(sql_type)


def _tipo_de_datos_rdf_comun_al_tipo_sql(sql_type):
    try:
        return _TIPOS_COMUNES_RDF_AL_TIPO_SQL[sql_type]
    except KeyError:
        datatype = _tipo_de_datos_rdf_comun_al_tipo_sql(sql_type.__mro__[1])
        _TIPOS_COMUNES_RDF_AL_TIPO_SQL[sql_type] = datatype
        return datatype

def rdf_datatypes_from_sql(sql_type):

    if not isinstance(sql_type, type):
        sql_type = sql_type.__class__

    return tuple(_rdf_datatypes_from_sql(sql_type))

#diccionario de tipos rdf a tipos literales de SQL
_SQL_LITERAL_TYPES_BY_RDF_DATATYPE = \
    {None: [_sqla.String],
     _rdf.XSD.boolean: [_sqla.Boolean],
     _rdf.XSD.date: [_sqla.Date],
     _rdf.XSD.dateTime: [_sqla.DateTime],
     _rdf.XSD.dayTimeDuration: [_sqla.Interval],
     _rdf.XSD.decimal: [_sqla.Numeric],
     _rdf.XSD.duration: [_sqla.Interval],
     _rdf.XSD.hexBinary: [_sqla.sql.sqltypes._Binary],
     _rdf.XSD.integer: [_sqla.Integer],
     _rdf.XSD.string: [_sqla.String],
     _rdf.XSD.time: [_sqla.Time],
     _rdf.XSD.yearMonthDuration: [_sqla.Interval],
     }

_RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE = \
    {_sqla.sql.sqltypes._Binary:
         lambda literal: _rdf.Literal(_bytes2hexstr(literal),
                                      datatype=_rdf.XSD.hexBinary),
     _sqla.Boolean: lambda literal: _rdf.Literal(literal),
     _sqla.Date: lambda literal: _rdf.Literal(literal),
     _sqla.DateTime: lambda literal: _rdf.Literal(literal),
     _sqla.Float: lambda literal: _rdf.Literal(literal),
     _sqla.Integer: lambda literal: _rdf.Literal(literal),
     #_sqla.Interval: _rdf_duration_from_timedelta,
     _sqla.Numeric: lambda literal: _rdf.Literal(literal),
     _sqla.String: lambda literal: _rdf.Literal(literal),
     _sqla.Time: lambda literal: _rdf.Literal(literal),
    # _sqla.sql.type_api.TypeEngine:
     #    lambda literal: _rdf.Literal(unicode(literal)),
     }

def _timedelta_from_rdf_duration(literal):
    
    match = _sdt.ISO8601_DURATION_RE.match(literal)

    if not match:
        raise ValueError('invalid RDF interval literal {!r}: expecting a'
                          ' literal that matches the format {!r}'
                          .format(literal, _sdt.ISO8601_DURATION_RE.pattern))

    return _timedelta(years=int(match.group('years') or 0),
                      months=int(match.group('months') or 0),
                      days=int(match.group('days') or 0),
                      hours=int(match.group('hours') or 0),
                      minutes=int(match.group('minutes') or 0),
                      seconds=int(match.group('seconds') or 0),
                      microseconds=(int(match.group('frac_seconds') or 0)
                                    * 1000000))


_SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE = \
    {_rdf.XSD.binary: lambda literal: _hexstr2bytes(literal),
     _rdf.XSD.boolean: lambda literal: literal.toPython(),
     _rdf.XSD.date: lambda literal: literal.toPython(),
     _rdf.XSD.dateTime: lambda literal: literal.toPython(),
     _rdf.XSD.decimal: lambda literal: literal.toPython(),
     _rdf.XSD.double: lambda literal: literal.toPython(),
     _rdf.XSD.duration: _timedelta_from_rdf_duration,
     _rdf.XSD.integer: lambda literal: literal.toPython(),
     _rdf.XSD.string: lambda literal: literal.toPython(),
     _rdf.XSD.time: lambda literal: literal.toPython(),
     }

def rdf_literal_from_sql(literal, sql_type):

    if not isinstance(sql_type, type):
        sql_type = sql_type.__class__

    return _rdf_literal_from_sql_func(sql_type)(literal)

def _rdf_literal_from_sql_func(sql_type):
    try:
        return _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sql_type]
    except KeyError:
        rdf_literal_from_sql = \
            _rdf_literal_from_sql_func(sql_type.__mro__[1])
        _RDF_LITERAL_FROM_SQL_FUNC_BY_SQL_TYPE[sql_type] = \
            rdf_literal_from_sql
        return rdf_literal_from_sql

def iri_safe(string):
    return _pct_encoded(string.encode('utf8'))


_RDF_DATATYPES_BY_SQL_TYPE = {}
for rdf_datatype, sql_types in _SQL_LITERAL_TYPES_BY_RDF_DATATYPE.items():
    for sql_type in sql_types:
        try:
            rdf_datatypes = _RDF_DATATYPES_BY_SQL_TYPE[sql_type]
        except KeyError:
            rdf_datatypes = []
            _RDF_DATATYPES_BY_SQL_TYPE[sql_type] = rdf_datatypes
        rdf_datatypes.append(rdf_datatype)
del rdf_datatype, rdf_datatypes, sql_type, sql_types
_RDF_DATATYPES_BY_SQL_TYPE[_sqla.sql.type_api.TypeEngine] = [None]



def _rdf_datatypes_from_sql(sql_type):
    try:
        return _RDF_DATATYPES_BY_SQL_TYPE[sql_type]
    except KeyError:
        datatype = _rdf_datatypes_from_sql(sql_type.__mro__[1])
        _RDF_DATATYPES_BY_SQL_TYPE[sql_type] = datatype
        return datatype


def sql_literal_from_rdf(literal):
    try:
        sql_literal_from_rdf_ = \
            _SQL_LITERAL_FROM_RDF_FUNC_BY_RDF_DATATYPE[literal.datatype]
    except KeyError:
        literal_py = literal.toPython()
        if isinstance(literal_py, _rdf.Literal):
            return str(literal)
        else:
            return literal_py
    else:
        return sql_literal_from_rdf_(literal)

def sql_literal_types_from_rdf(datatype):
    return tuple(_SQL_LITERAL_TYPES_BY_RDF_DATATYPE.get(datatype,
                                                        (_sqla.String,)))
