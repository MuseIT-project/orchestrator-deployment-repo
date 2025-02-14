import json
from rdflib import Graph, Literal, Namespace, RDF
from urllib.parse import quote
import pydot

with open('foundkeys_origin.json', 'r') as json_file:
    foundkeys = json.load(json_file)

EX = Namespace("http://transformations.museit.eu/")
g = Graph()
g.bind("ex", EX)

artist_set = set()
artists = []

for item in foundkeys:
    artist_set.add(item['artistName'])

item_index = 0
for item in artist_set:
    artists.append({
        "artistName": item,
        "id": f"artist_{item_index}"
    })
    item_index += 1

artworks = []

styles = set()
styles_list = []
for item in foundkeys:
    styles.add(item['style'])

item_index = 0
for item in styles:
    styles_list.append({
        "style": item,
    })
    item_index += 1

print(styles_list)


for item in foundkeys:
    artworks.append({
        "id": item['title'],
        "title": item['title'],
        "style": item['style'],
        "completion_year": item['yearAsString'],
        "artist": item['artistName'],
        "description": item['ollama_description'],
        "keywords": item['keywords']
    })


for artist in artists:
    artist_uri = EX[quote(str(artist['id']))]
    g.add((artist_uri, RDF.type, EX.Artist))
    g.add((artist_uri, EX.artistName, Literal(artist['artistName'])))

for artwork in artworks:
    artwork_uri = EX[quote(str(artwork['id']))]
    g.add((artwork_uri, RDF.type, EX.Artwork))
    g.add((artwork_uri, EX.title, Literal(artwork['title'])))
    g.add((artwork_uri, EX.style, EX[quote(str(artwork['style']))]))
    g.add((artwork_uri, EX.completion_year, Literal(artwork['completion_year'])))
    g.add((artwork_uri, EX.description, Literal(artwork['description'])))
    g.add((artwork_uri, EX.keywords, Literal(artwork['keywords'])))
    g.add((artwork_uri, EX.artist, EX[quote(str(artwork['artist']))]))

for style in styles_list:
    style_uri = EX[quote(str(style['style']))]
    g.add((style_uri, RDF.type, EX.Style))
    g.add((style_uri, EX.style, Literal(style['style'])))
    

for artwork in artworks:
    artist_uri = EX[quote(str(artwork['artist']))]
    artwork_uri = EX[quote(str(artwork['id']))]
    style_uri = EX[quote(str(artwork['style']))]
    g.add((artist_uri, EX.createdBy, artwork_uri))
    g.add((style_uri, EX.hasStyle, artwork_uri))

# Add root node
root_uri = EX["MuseIt"]
g.add((root_uri, RDF.type, EX.Root))

# Relate root node to top-level nodes
g.add((root_uri, EX.hasCategory, EX["Artworks"]))
g.add((root_uri, EX.hasCategory, EX["Artists"]))
g.add((root_uri, EX.hasCategory, EX["Styles"]))

#create top level hierarchy
g.add((EX["Artworks"], RDF.type, EX.Artworks))
g.add((EX["Artists"], RDF.type, EX.Artists))
g.add((EX["Styles"], RDF.type, EX.Styles))

g.serialize(destination="museit_topic_graph.rdf", format="application/rdf+xml")


