"""Debug: trace token removal for Quduri record."""

import json
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
import matching.config as cfg

# Load data
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Extract just Quduri record
quduri_bnf_id = "OAI_11000520"
test_bnf_records = {quduri_bnf_id: all_bnf[quduri_bnf_id]}

print(f"BNF Record: {quduri_bnf_id}")
quduri_record = test_bnf_records[quduri_bnf_id]
print(f"\nBefore token removal:")
print(f"  title_lat: {quduri_record.title_lat}")
print(f"  title_ara: {quduri_record.title_ara}")
print(f"  creator_lat: {quduri_record.creator_lat}")
print(f"  creator_ara: {quduri_record.creator_ara}")

# Run stage 1 matching
print(f"\nRunning stage 1 (author matching)...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))

# Execute only stage 1
stage1 = pipeline.stages[0]
stage1.execute(pipeline)

# Check results
matched_authors = pipeline.get_stage1_result(quduri_bnf_id)
matched_candidates = pipeline.get_stage1_matched_candidates(quduri_bnf_id)

print(f"\nStage 1 results:")
print(f"  Matched authors: {matched_authors}")
print(f"  Matched candidates: {matched_candidates}")

if matched_candidates:
    print(f"\nMatched candidate tokens:")
    for candidate in matched_candidates:
        tokens = candidate.lower().split()
        print(f"  Candidate: '{candidate}'")
        print(f"  Tokens: {tokens}")

# Now show token removal
print(f"\nApplying token removal...")

# Manually apply token removal as the pipeline would
author_tokens = set()
for candidate in matched_candidates:
    tokens = candidate.lower().split()
    author_tokens.update(tokens)

print(f"  All author tokens to remove: {author_tokens}")

# Apply to each field
fields_to_check = ["title_lat", "title_ara", "creator_lat", "creator_ara",
                   "description_candidates_lat", "description_candidates_ara"]

for field in fields_to_check:
    if not hasattr(quduri_record, field):
        continue

    field_value = getattr(quduri_record, field, None)
    if not isinstance(field_value, list):
        continue

    print(f"\n  Field: {field}")
    for i, value in enumerate(field_value):
        if not isinstance(value, str):
            continue

        print(f"    Original: '{value}'")

        # Tokenize and remove
        text_tokens = value.split()
        cleaned = [t for t in text_tokens if t.lower() not in author_tokens]
        cleaned_value = " ".join(cleaned)

        print(f"    Cleaned:  '{cleaned_value}'")
        if cleaned_value != value:
            print(f"    Tokens removed: {set(text_tokens) - set(cleaned)}")
        else:
            print(f"    No change")

print(f"\nAfter token removal:")
print(f"  title_lat: {quduri_record.title_lat}")
print(f"  title_ara: {quduri_record.title_ara}")
print(f"  creator_lat: {quduri_record.creator_lat}")
print(f"  creator_ara: {quduri_record.creator_ara}")
