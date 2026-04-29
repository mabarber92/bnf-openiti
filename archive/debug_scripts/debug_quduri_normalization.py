"""Debug: trace normalization and token removal for Quduri record."""

import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.normalize import normalize_for_matching
import matching.config as cfg

# Load data
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

quduri_bnf_id = "OAI_11000520"
quduri_record = all_bnf[quduri_bnf_id]

print(f"BNF Record: {quduri_bnf_id}\n")

# Matched candidates (what stage 1 found)
matched_candidates = ['ahmad ibn muhammad al quduri', 'ahmad ibn muhammad al quduri.']

print(f"Matched candidates from stage 1:")
for cand in matched_candidates:
    print(f"  '{cand}'")

# Build token set
author_tokens = set()
for candidate in matched_candidates:
    tokens = candidate.lower().split()
    author_tokens.update(tokens)

print(f"\nAuthor tokens to remove: {author_tokens}")
print(f"  Contains 'quduri': {'quduri' in author_tokens}")

# Now test normalization and removal on each field
print(f"\n{'='*80}")
print(f"Field-by-field normalization and token removal:")
print(f"{'='*80}")

fields_to_check = ["title_lat", "title_ara", "creator_lat", "creator_ara",
                   "description_candidates_lat", "description_candidates_ara"]

for field in fields_to_check:
    if not hasattr(quduri_record, field):
        continue

    field_value = getattr(quduri_record, field, None)
    if not isinstance(field_value, list):
        continue

    print(f"\n{field}:")
    for i, value in enumerate(field_value):
        if not isinstance(value, str):
            continue

        print(f"\n  [{i}] Original: '{value}'")

        # Normalize
        norm_value = normalize_for_matching(value, split_camelcase=False, is_openiti=False)
        print(f"      Normalized: '{norm_value}'")

        # Check which tokens are in normalized value
        norm_tokens = norm_value.lower().split()
        print(f"      Tokens in normalized: {norm_tokens}")

        # Find which author tokens are present
        present_tokens = [t for t in norm_tokens if t.lower() in author_tokens]
        print(f"      Author tokens present: {present_tokens}")
        print(f"        Contains 'quduri': {'quduri' in present_tokens}")

        # Remove tokens
        cleaned = [t for t in norm_tokens if t.lower() not in author_tokens]
        cleaned_value = " ".join(cleaned)

        print(f"      After removal: '{cleaned_value}'")
        if cleaned_value != norm_value:
            removed = set(norm_tokens) - set(cleaned)
            print(f"      Tokens removed: {removed}")
        else:
            print(f"      No tokens removed")
