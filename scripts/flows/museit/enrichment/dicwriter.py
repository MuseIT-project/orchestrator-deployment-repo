from csv import DictWriter
import json

with open("foundkeys_merged.json", "r") as file:
    data = json.load(file)

output = []

for item in data:
    caption = str(item.get("ollama_description").strip().replace("\n", " "))
    output.append({
        "title": item.get("title"),
        "style": item.get("style"),
        "artist name": item.get("artistName"),
        "completion year": item.get("completitionYear"),
        "captions": caption
    })

with open("output_2.csv", 'w') as file:
    writer = DictWriter(file, fieldnames=output[0].keys())
    writer.writeheader()
    writer.writerows(output)