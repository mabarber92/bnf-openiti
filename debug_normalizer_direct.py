"""
Direct test of normalizer functions.

Call the normalizer functions directly without relying on config/reloading.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from matching.normalize import (
    normalize_transliteration,
    _apply_openiti_conversions,
)
from matching.normalize_diacritics import normalize_with_diacritics

# Load sample BNF records to test on
from parsers.bnf import load_bnf_records
import matching.config as cfg

all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
bnf_list = list(all_bnf.items())[:10]

print("="*120)
print("DIRECT NORMALIZER TEST")
print("="*120)

for idx, (bnf_id, record) in enumerate(bnf_list, 1):
    creators_lat = getattr(record, "creator_lat", []) or []
    creator = creators_lat[0] if creators_lat else ""

    if not creator or len(creator) < 5:
        continue

    print(f"\n{idx}. Original: {creator[:70]}")
    print(f"   Length: {len(creator)}, Has diacritics: {any(ord(c) > 127 for c in creator)}\n")

    # Step 1: OpenITI conversions only
    step1 = _apply_openiti_conversions(creator)
    print(f"   Step 1 (OpenITI conversions):")
    print(f"     {step1[:70]}")
    print(f"     Length: {len(step1)}\n")

    # Step 2a: With table=True (on)
    step2_on = normalize_with_diacritics(step1, use_table=True)
    print(f"   Step 2 (with diacritics table ON):")
    print(f"     {step2_on[:70]}")
    print(f"     Length: {len(step2_on)}\n")

    # Step 2b: With table=False (off)
    step2_off = normalize_with_diacritics(step1, use_table=False)
    print(f"   Step 2 (with diacritics table OFF):")
    print(f"     {step2_off[:70]}")
    print(f"     Length: {len(step2_off)}\n")

    # Check difference
    if step2_on == step2_off:
        print(f"   ✓ SAME OUTPUT")
    else:
        print(f"   ✗ DIFFERENT OUTPUT")
        print(f"     ON  (len={len(step2_on)}): {repr(step2_on[:80])}")
        print(f"     OFF (len={len(step2_off)}): {repr(step2_off[:80])}")

    # Step 3: Final normalization (legacy)
    final_on = normalize_transliteration(step2_on)
    final_off = normalize_transliteration(step2_off)

    print(f"\n   Step 3 (legacy normalizer):")
    print(f"     With table ON:  {final_on[:70]}")
    print(f"     With table OFF: {final_off[:70]}")

    if final_on == final_off:
        print(f"     ✓ FINAL SAME")
    else:
        print(f"     ✗ FINAL DIFFERENT")
        print(f"       ON  (len={len(final_on)}): {repr(final_on[:80])}")
        print(f"       OFF (len={len(final_off)}): {repr(final_off[:80])}")

print(f"\n{'='*120}")
print("CONVERSION TABLE CONTENTS")
print(f"{'='*120}")

from matching.normalize_diacritics import get_conversion_table
conversions, is_populated = get_conversion_table()

if is_populated:
    print(f"✓ Conversion table loaded with {len(conversions)} mappings")
    print(f"\nSample mappings:")
    for i, (char, equiv) in enumerate(list(conversions.items())[:10]):
        print(f"  '{char}' (U+{ord(char):04X}) → '{equiv}'")
else:
    print(f"✗ Conversion table is EMPTY or NOT POPULATED")
