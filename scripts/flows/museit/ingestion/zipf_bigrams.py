import re
import json
import csv
from collections import Counter
import nltk
from nltk import pos_tag
from nltk.corpus import wordnet

# === INITIAL SETUP (only once) ===
# nltk.download('averaged_perceptron_tagger')
# nltk.download('wordnet')

# === Load Descriptions ===
with open("foundkeys_enriched_34b_hsi_keywords.json", "r") as f:
    data = json.load(f)
    descriptions = [item['ollama_description_34b_2'] for item in data if 'ollama_description_34b_2' in item]

# === Tokenization ===
def tokenize(text):
    text = text.lower()
    return re.findall(r'\b\w+\b', text)

# === POS Mapping (Penn → WordNet-style tag) ===
def penn_to_wordnet(tag):
    if tag.startswith('J'):
        return 'adj'
    elif tag.startswith('V'):
        return 'verb'
    elif tag.startswith('N'):
        return 'noun'
    elif tag.startswith('R'):
        return 'adv'
    else:
        return 'other'

# === Aggregate Bigrams ===
bigrams = []
for desc in descriptions:
    tokens = tokenize(desc)
    if len(tokens) < 2:
        continue
    # Create bigrams from tokens
    for i in range(len(tokens) - 1):
        w1, w2 = tokens[i], tokens[i+1]
        bigrams.append((w1, w2))

# === Frequency Count ===
freq = Counter(bigrams)
total_bigrams = sum(freq.values())

# === POS Tagging ===
unique_words = list(set([w for bigram in freq.keys() for w in bigram]))
tagged_words = pos_tag(unique_words)
pos_lookup = {word: penn_to_wordnet(tag) for word, tag in tagged_words}

# === Prepare Data for CSV ===
rows = []
for (w1, w2), count in freq.most_common():
    phrase = f"{w1} {w2}"
    percentage = (count / total_bigrams) * 100
    pos_tag_pair = f"{pos_lookup.get(w1, 'unknown')},{pos_lookup.get(w2, 'unknown')}"
    rows.append([phrase, count, round(percentage, 4), pos_tag_pair])

# === Write to CSV ===
output_file = "zipf_bigrams_with_pos.csv"
with open(output_file, mode="w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["bigrams", "count", "percentage", "pos"])
    writer.writerows(rows)

print(f"✅ Done. Output written to: {output_file}")
