#!/usr/bin/env python3

import json

def isolate_title_and_ollama_description(filepath, output_file):
    result = []
    with open(filepath, 'r') as file:
        data = json.load(file)
    for item in data:
        result.append({'title': item['title'], 'ollama_description': item['ollama_description']})
    with open(output_file, 'w') as file:
        json.dump(result, file, indent=4)

if __name__ == '__main__':
    isolate_title_and_ollama_description('foundkeys_enriched.json', 'foundkeys_isolated.json')
