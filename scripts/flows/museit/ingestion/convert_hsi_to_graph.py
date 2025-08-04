import json
import re
from rdflib import Graph, Namespace, Literal, RDF, URIRef

# Namespaces
BASE = Namespace("http://example.org/")
ART = Namespace(BASE + "artwork/")
ARTIST = Namespace(BASE + "artist/")
REL = Namespace(BASE + "relation/")

INPUT_JSON = "foundkeys_enriched_34b_hsi_keywords.json"
OUTPUT_TTL = "artworks.ttl"

relations = {
    "Age": REL.SubjectAgeIs,
    "Age List": REL.SubjectAgeIs,
    "Time": REL.TimeOfDayIs,
    "Time List": REL.TimeOfDayIs,
    "Type": REL.TypeOf,
    "Type List": REL.TypeOf,
    "People": REL.portrayed,
    "People List": REL.portrayed,
}

def sanitize(text):
    return re.sub(r"[^a-zA-Z0-9]", "_", text.strip())

def parse_hsi_field(hsi_raw):
    if not hsi_raw:
        return {}
    try:
        return json.loads(hsi_raw.strip())
    except Exception:
        return {}

def main():
    g = Graph()
    g.bind("base", BASE)
    g.bind("art", ART)
    g.bind("artist", ARTIST)
    g.bind("rel", REL)

    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        title = item.get("title")
        artist_name = item.get("artistName", "").strip()
        hsi_field = item.get("hsi_34b", "")

        if not title or not artist_name:
            continue

        art_id = sanitize(title)
        artist_id = sanitize(artist_name)

        art_uri = ART[art_id]
        artist_uri = ARTIST[artist_id]

        # Define Artwork
        g.add((art_uri, RDF.type, BASE.Artwork))
        g.add((art_uri, BASE.title, Literal(title)))
        g.add((art_uri, REL.createdBy, artist_uri))

        # Define Artist (only once)
        g.add((artist_uri, RDF.type, BASE.Artist))
        g.add((artist_uri, BASE.name, Literal(artist_name)))
        g.add((artist_uri, REL.isCreator, art_uri))

        # HSI relations
        hsi_dict = parse_hsi_field(hsi_field)
        for key, pred in relations.items():
            value = hsi_dict.get(key)
            if value and value.strip().lower() != "none":
                obj = URIRef(BASE + sanitize(value))
                g.add((art_uri, pred, obj))

    # Serialize to TTL
    g.serialize(destination=OUTPUT_TTL, format="turtle")

if __name__ == "__main__":
    main()
