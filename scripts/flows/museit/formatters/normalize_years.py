import re
from statistics import mean

ROMAN_NUMERAL_MAP = {
    'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
    'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
    'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15,
    'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19,
    'XX': 20, 'XXI': 21, 'XXII': 22
}

def roman_to_century(roman):
    return ROMAN_NUMERAL_MAP.get(roman, None)

def century_to_year_midpoint(century_int):
    return (century_int - 1) * 100 + 50

def normalize_year(value):
    value = value.strip()
    
    # Case 1: Range of years, e.g., 1475-1480
    if re.match(r'^\d{4}-\d{4}$', value):
        start, end = map(int, value.split('-'))
        return round(mean([start, end]))

    # Case 2: Single year
    if re.match(r'^\d{4}$', value):
        return int(value)

    # Case 3: Century e.g., "XV cent." or "XV-XVI cent."
    match = re.match(r'^([IVXLCDM]+)(?:-([IVXLCDM]+))?\s*cent\.$', value)
    if match:
        c1 = roman_to_century(match.group(1))
        c2 = roman_to_century(match.group(2)) if match.group(2) else None
        
        if c1 and not c2:
            return century_to_year_midpoint(c1)
        elif c1 and c2:
            midpoints = [century_to_year_midpoint(c) for c in (c1, c2)]
            return round(mean(midpoints))

    return None  # unknown format