from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("http://localhost:7200/repositories/museit_topic_graph")

query = """
PREFIX ex: <http://transformations.museit.eu/>
SELECT ?title ?description
WHERE {
    ?artwork a ex:Artwork .
    ?artwork ex:title ?title .
    ?artwork ex:description ?description .
}
"""

sparql.setQuery(query)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

for result in results["results"]["bindings"]:
    title = result["title"]["value"]
    description = result["description"]["value"]
    print(f"Title: {title}, Description: {description}")
