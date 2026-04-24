#!/usr/bin/env python3
"""
Extract all special/non-ASCII characters from BNF parsed JSON.
Output as CSV for manual review and conversion table creation.
"""

import json
import unicodedata
import csv
from pathlib import Path
from collections import defaultdict

def get_char_info(char):
    """Get Unicode name and category for a character."""
    try:
        name = unicodedata.name(char)
    except ValueError:
        name = "UNKNOWN"
    category = unicodedata.category(char)
    return name, category

def extract_special_chars(json_file):
    """Extract all non-ASCII characters from BNF records."""
    with open(json_file, encoding='utf-8', errors='replace') as f:
        bnf_data = json.load(f)

    special_chars = defaultdict(int)
    char_contexts = defaultdict(list)

    # Scan all text fields
    for bnf_id, record in bnf_data.items():
        fields_to_scan = [
            'title_lat', 'title_ara',
            'creator_lat', 'creator_ara',
            'description_lat', 'description_ara',
            'description_candidates_lat', 'description_candidates_ara',
            'contributor_lat', 'contributor_ara',
            'subject',
        ]

        for field in fields_to_scan:
            if field not in record:
                continue

            values = record[field]
            if not isinstance(values, list):
                values = [values] if values else []

            for value in values:
                if not isinstance(value, str):
                    continue

                for i, char in enumerate(value):
                    if ord(char) > 127:
                        special_chars[char] += 1
                        if len(char_contexts[char]) < 2:
                            start = max(0, i - 10)
                            end = min(len(value), i + 15)
                            context = value[start:end]
                            char_contexts[char].append((field, context))

    return special_chars, char_contexts

def main():
    json_file = Path("outputs/bnf_parsed.json")

    if not json_file.exists():
        print(f"Error: {json_file} not found")
        return

    print("Extracting special characters from BNF data...")
    special_chars, char_contexts = extract_special_chars(json_file)

    # Sort by frequency
    sorted_chars = sorted(special_chars.items(), key=lambda x: x[1], reverse=True)

    # Write CSV
    output_file = Path("data_samplers/bnf_diacritic_conversions.csv")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'character',
            'unicode_code',
            'unicode_name',
            'category',
            'frequency',
            'context_1',
            'context_2',
            'openiti_equivalent',
            'notes'
        ])

        for char, count in sorted_chars:
            name, category = get_char_info(char)
            code = f"U+{ord(char):04X}"

            contexts = char_contexts.get(char, [])
            context1 = contexts[0][1] if len(contexts) > 0 else ""
            context2 = contexts[1][1] if len(contexts) > 1 else ""

            writer.writerow([
                char,
                code,
                name,
                category,
                count,
                context1,
                context2,
                '',  # Empty for manual review
                ''   # Empty for notes
            ])

    print(f"\nConversion table written to: {output_file}")
    print(f"Total special characters: {len(sorted_chars)}")
    print(f"\nEdit the 'openiti_equivalent' column to specify conversions.")
    print(f"Leave blank to use standard normalization.")

if __name__ == "__main__":
    main()
