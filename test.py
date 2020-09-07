#$ python -m django startproject mysite
#mysite es el sitio nuevo que voy a crear
#$ python manage.py startapp polls
#crea una nueva aplicacion polls encuestas
#python manage.py runserver
#ejecuta el servidor para ver en el navegador el programa
#python manage.py migrate




#es importate que en el documento cuando hable de sqalchemy, busque sqalchemy.inspect 

#en el metodo open, me conecto con la bd, creo toda la arquitectura del orm, obtengo los nombres de las tablas,
#entonces comienzo a trabajar en cada una de las tablas usando sqalchemy.inspect, obtengo el diccionario que contirne por
#cada tabla una coleccion con sus respectivas propiedades y su tipo de la biblioteca rdflib correspondiente a la conversion
#del tipo sql, ademas obtengo por cada tabla las relaciones que tiene con alguna otra tabla en este diccionario se guarda
#el (nombre_de_la_tabla,{set_de_relaciones} donde set_de_relaciones contiene primero el identificador de la columna que
#es la llave foranea de la relacion y luego un objeto relationshipProperty junto con el nombre de la tabla con el cual se 
#relaciona

#en el metodo _table_type_triples, entra el iri de una tabla, obtiene la llave primary de la tabla y realiza una query 
#por esa fila que representa la primary_key de la tabla, obtiene los valores e imprime esto
#aclaracion en este metodo, si la tabla no tiene una llae primaria ella construye en vez de estas direcciones URI que 
#salen en el ejemplo, construye un BNode o blanck node
#salida estudiantes/id=1 http://www.w3.org/1999/02/22-rdf-syntax-ns#type estudiantes

#el metodo _table_allpredicates_triples(self, table_iri, object_pattern) si el object_pattern is none
#se encarga de por cada una de las filas de la tabla devolver las triplas que contengan las relaciones con otras filas de
#otras tablas, tb se encarga de devolver los valores de las propiedades y sus respectivos tipos en rdf
#ejemplo
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), rdflib.term.URIRef('estudiantes')) None
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('estudiantes#id'), rdflib.term.Literal('5', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#integer'))) None
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('estudiantes#name'), rdflib.term.Literal('Fermin Lazaro')) None
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('estudiantes#apellidos'), rdflib.term.Literal('Felipe Tame')) None
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('estudiantes#escuela_id'), rdflib.term.Literal('2', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#integer'))) None
#(rdflib.term.URIRef('estudiantes/id=5'), rdflib.term.URIRef('estudiantes#ref-escuela_id'), rdflib.term.URIRef('Escuela/id=2')) None
