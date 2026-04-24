#!/usr/bin/env python3
"""
Generate diacritic conversion table for a manuscript library.

Part of the ingestion workflow: when adding a new manuscript library,
run this script to extract all special characters and create a baseline
conversion table ready for manual review.

Usage:
    python utils/generate_diacritic_conversions.py --library bnf --json outputs/bnf_parsed.json
    python utils/generate_diacritic_conversions.py --library chester_beatty --json outputs/chester_beatty_parsed.json
"""

import argparse
import json
import unicodedata
import csv
from pathlib import Path
from collections import defaultdict


def extract_special_chars_from_json(json_path: str) -> dict:
    """
    Extract all non-ASCII characters from a manuscript library JSON.

    Parameters
    ----------
    json_path : str
        Path to the parsed JSON file (e.g., bnf_parsed.json)

    Returns
    -------
    dict
        {character: {'count': int, 'name': str, 'category': str}}
    """
    special_chars = {}

    # Stream read to handle large files
    with open(json_path, encoding='utf-8', errors='ignore') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON: {e}")
            return {}

    # Scan all text fields
    for record_id, record in data.items():
        if not isinstance(record, dict):
            continue

        # Fields to scan for special characters
        text_fields = [
            'title_lat', 'title_ara',
            'creator_lat', 'creator_ara',
            'description_lat', 'description_ara',
            'description_candidates_lat', 'description_candidates_ara',
            'contributor_lat', 'contributor_ara',
            'subject',
        ]

        for field in text_fields:
            if field not in record:
                continue

            values = record[field]
            if not isinstance(values, list):
                values = [values] if values else []

            for value in values:
                if not isinstance(value, str):
                    continue

                for char in value:
                    if ord(char) > 127:  # Non-ASCII
                        if char not in special_chars:
                            try:
                                name = unicodedata.name(char)
                            except ValueError:
                                name = "UNKNOWN"
                            category = unicodedata.category(char)

                            special_chars[char] = {
                                'count': 0,
                                'name': name,
                                'category': category,
                            }

                        special_chars[char]['count'] += 1

    return special_chars


def create_baseline_table(special_chars: dict, output_path: str) -> None:
    """
    Create baseline conversion table CSV.

    Parameters
    ----------
    special_chars : dict
        {character: {'count': int, 'name': str, 'category': str}}
    output_path : str
        Path to write the CSV
    """
    # Sort by frequency (most common first)
    sorted_chars = sorted(
        special_chars.items(),
        key=lambda x: x[1]['count'],
        reverse=True
    )

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'character',
            'unicode_code',
            'unicode_name',
            'category',
            'openiti_equivalent',
            'notes'
        ])

        for char, info in sorted_chars:
            code = f"U+{ord(char):04X}"
            writer.writerow([
                char,
                code,
                info['name'],
                info['category'],
                '',  # openiti_equivalent - to be filled manually
                f"Frequency: {info['count']}",
            ])


def main():
    parser = argparse.ArgumentParser(
        description="Generate diacritic conversion table for a manuscript library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python utils/generate_diacritic_conversions.py --library bnf --json outputs/bnf_parsed.json
  python utils/generate_diacritic_conversions.py --library chester_beatty --json outputs/chester_beatty_parsed.json

The script extracts all special characters from the manuscript library JSON and
creates a baseline CSV ready for manual review. Fill in the 'openiti_equivalent'
column with the desired conversion (or leave blank to remove the character).
        """,
    )

    parser.add_argument(
        '--library',
        required=True,
        help='Name of the manuscript library (e.g., bnf, chester_beatty)',
    )
    parser.add_argument(
        '--json',
        required=True,
        help='Path to the parsed JSON file (e.g., outputs/bnf_parsed.json)',
    )

    args = parser.parse_args()

    json_path = Path(args.json)
    if not json_path.exists():
        print(f"Error: JSON file not found: {json_path}")
        return 1

    output_path = Path("data") / f"{args.library}_diacritic_conversions.csv"

    print(f"Extracting special characters from {json_path}...")
    special_chars = extract_special_chars_from_json(str(json_path))

    if not special_chars:
        print("No special characters found")
        return 1

    print(f"Found {len(special_chars)} unique special characters")
    print(f"Creating baseline table: {output_path}")

    create_baseline_table(special_chars, str(output_path))

    print(f"\n{'='*80}")
    print("CONVERSION TABLE CREATED")
    print(f"{'='*80}")
    print(f"\nLocation: {output_path}")
    print(f"Total characters: {len(special_chars)}")
    print(f"\nNext steps:")
    print(f"1. Open {output_path} in a spreadsheet editor")
    print(f"2. Fill in the 'openiti_equivalent' column:")
    print(f"   - For conversions: enter the replacement (e.g., 'gh' for ǧ)")
    print(f"   - For removals: leave blank")
    print(f"   - For preservation: enter the character itself (e.g., ʿ for ayn)")
    print(f"3. Save and commit to git")
    print(f"4. The normalizer will automatically use it when processing {args.library} records")

    return 0


if __name__ == "__main__":
    exit(main())
