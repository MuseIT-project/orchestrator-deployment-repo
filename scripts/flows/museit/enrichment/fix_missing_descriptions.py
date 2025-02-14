import json

with open('foundkeys_enriched_new.json', 'r') as f:
    foundkeys_new = json.load(f)

with open('foundkeys_missing_indented.json', 'r') as f:
    foundkeys_indented = json.load(f)

for item in foundkeys_indented:
    for item2 in foundkeys_new:
        if item['title'] == item2['title']:
            foundkeys_indented.remove(item)
            foundkeys_indented.append(item2)

with open('foundkeys_merged.json', 'w') as f:
    json.dump(foundkeys_indented, f, indent=4)