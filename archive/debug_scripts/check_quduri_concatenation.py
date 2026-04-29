import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')

from parsers.openiti import load_openiti_corpus
from matching.candidate_builders import build_author_candidates_by_script
from matching.normalize import normalize_for_matching
import matching.config as cfg

openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
quduri_data = openiti_data['authors']['0428AbuHusaynQuduri']

candidates = build_author_candidates_by_script(quduri_data)

print("QUDURI CONCATENATION CHECK")
print("=" * 100)
print()

for script in ["lat", "ara"]:
    print(f"Script: {script}")
    print(f"Raw candidates: {candidates[script]}")
    print()
    
    normalized = []
    for candidate in candidates.get(script, []):
        if not candidate:
            continue
        norm = normalize_for_matching(candidate, split_camelcase=True, is_openiti=True)
        normalized.append(norm)
        print(f"  {candidate[:50]:50} → {norm[:50]}")
    
    concatenated = " ".join(normalized)
    print(f"\nConcatenated: {concatenated}")
    print()

