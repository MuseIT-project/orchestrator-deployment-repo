import re
import json
import csv
from collections import Counter
import nltk
from nltk import pos_tag
from nltk.corpus import wordnet

# === INITIAL SETUP (run once if not already done) ===
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')

# === Load Descriptions ===
with open("foundkeys_enriched_34b_hsi_keywords.json", "r") as f:
    data = json.load(f)
    descriptions = [item['ollama_description_34b_2'] for item in data if 'ollama_description_34b_2' in item]

# === Tokenization ===
def tokenize(text):
    text = text.lower()
    return re.findall(r'\b\w+\b', text)

# === POS Mapping (Penn → WordNet-style) ===
def penn_to_wordnet(pos_tag):
    if pos_tag.startswith('J'):
        return 'adj'
    elif pos_tag.startswith('V'):
        return 'verb'
    elif pos_tag.startswith('N'):
        return 'noun'
    elif pos_tag.startswith('R'):
        return 'adv'
    else:
        return 'other'

# === Aggregate Tokens ===
all_tokens = []
for desc in descriptions:
    all_tokens.extend(tokenize(desc))

# === Count Frequencies ===
freq = Counter(all_tokens)
total_words = sum(freq.values())

# === POS Tagging ===
unique_words = list(freq.keys())
tagged_words = pos_tag(unique_words)
pos_lookup = {word: penn_to_wordnet(tag) for word, tag in tagged_words}

# === Prepare Data ===
rows = []
for word, count in freq.most_common():
    percentage = (count / total_words) * 100
    pos = pos_lookup.get(word, 'unknown')
    rows.append([word, count, round(percentage, 4), pos])

# === Write to CSV ===
output_file = "zipf_output_with_pos.csv"
with open(output_file, mode="w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["word", "count", "percentage", "pos"])
    writer.writerows(rows)

print(f"✅ Done. Output written to: {output_file}")
