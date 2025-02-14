#!/usr/bin/env python3

def convert_to_textline(filepath, output_file):
    import json
    with open(filepath, 'r') as file:
        data = json.load(file)
    with open(output_file, 'w') as file:
        for item in data:
            file.write(strip_newlines_from_description(item['ollama_description']) + '\n')

def strip_newlines_from_description(description):
    return description.replace('\n', ' ')

if __name__ == '__main__':
    convert_to_textline('foundkeys_enriched.json', 'foundkeys_textline.txt')
