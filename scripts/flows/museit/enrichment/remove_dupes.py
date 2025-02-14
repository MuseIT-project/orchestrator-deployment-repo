import json

valid_results = []

with open('foundkeys_with_global_ids.json', 'r') as f:
    foundkeys = json.load(f)

print(len(foundkeys))

