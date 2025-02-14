import csv
import json

def open_foundkeys():
    with open('foundkeys_enriched_indented.json', 'r') as f:
        foundkeys = json.load(f)
    return foundkeys

def convert_to_csv(foundkeys):
    with open('foundkeys.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        keys = foundkeys[0].keys()
        writer.writerow(keys)
        for item in foundkeys:  
            writer.writerow(item.values())

if __name__ == '__main__':
    foundkeys = open_foundkeys()
    convert_to_csv(foundkeys)