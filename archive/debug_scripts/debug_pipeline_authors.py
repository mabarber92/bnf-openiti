"""Debug: show actual author URIs returned by pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher

bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

test_record_id = "OAI_10030933"
bnf_records_test = {test_record_id: bnf_records[test_record_id]}

pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="debug", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))

pipeline.run()

s1_results = pipeline.get_stage1_result(test_record_id)
print(f"Stage 1 returned {len(s1_results or [])} author URIs")
if s1_results:
    for author_uri in list(s1_results)[:10]:
        print(f"  {author_uri}")
    if len(s1_results) > 10:
        print(f"  ... and {len(s1_results) - 10} more")
