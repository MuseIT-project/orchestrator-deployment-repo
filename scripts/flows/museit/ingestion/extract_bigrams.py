import pandas as pd
import json

# === CONFIGURATION ===
input_excel_file = "zipf_bigrams_with_pos.csv"  # Replace with your actual file name
output_json_file = "adj_noun_bigrams.json"

# === READ CSV FILE ===
df = pd.read_csv(input_excel_file)

# === FILTER FOR 'adj,noun' IN POS COLUMN ===
filtered_df = df[df['pos'] == 'adj,noun']

# === EXTRACT BIGRAMS COLUMN ===
bigrams_list = filtered_df['bigrams'].tolist()

# === SAVE TO JSON FILE ===
with open(output_json_file, 'w') as f:
    json.dump(bigrams_list, f, indent=4)

print(f"Saved {len(bigrams_list)} bigrams to {output_json_file}")
