"""
Debug script: understand why IbnHajar creator field matching isn't working
"""

import sys
sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg

# Load data
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

# Get the IbnHajar record
book_uri = 'OAI_11001075'
bnf_id = '0852IbnHajarCasqalani.InbaGhumr'
bnf_record = all_bnf.get(book_uri)

print(f"BNF Record: {bnf_id}")
print(f"Book URI: {book_uri}")
print(f"BNF Record found: {bnf_record is not None}")

if bnf_record:
    creator_lat = getattr(bnf_record, 'creator_lat', None)
    creator_ara = getattr(bnf_record, 'creator_ara', None)

    print(f"\nCreator fields:")
    print(f"  creator_lat type: {type(creator_lat).__name__}")
    print(f"  creator_lat has value: {bool(creator_lat)}")
    if creator_lat:
        if isinstance(creator_lat, list):
            print(f"  creator_lat is list of {len(creator_lat)} items")
            for i in range(min(3, len(creator_lat))):
                print(f"    [{i}]: (Unicode content - {len(creator_lat[i])} chars)")
        else:
            print(f"  creator_lat is string of {len(creator_lat)} chars")

    print(f"\n  creator_ara type: {type(creator_ara).__name__}")
    print(f"  creator_ara has value: {bool(creator_ara)}")
    if creator_ara:
        if isinstance(creator_ara, list):
            print(f"  creator_ara is list of {len(creator_ara)} items")
        else:
            print(f"  creator_ara is string of {len(creator_ara)} chars")

# Now run the pipeline and check what happened
print("\n\nRunning pipeline...")
test_bnf_records = {book_uri: bnf_record} if bnf_record else {}
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Check stage 1 scores
print(f"\nStage 1 results for {bnf_id}:")
stage1_authors = pipeline.get_stage1_result(book_uri) or []
stage1_scores_post = pipeline.get_stage1_scores(book_uri) or {}
stage1_scores_pre = getattr(pipeline, '_stage1_scores_pre_reweighting', {}).get(book_uri, {})

print(f"  Authors matched: {len(stage1_authors)}")
for author_uri in stage1_authors:
    pre = stage1_scores_pre.get(author_uri, 0)
    post = stage1_scores_post.get(author_uri, 0)
    print(f"    {author_uri}: pre={pre:.3f}, post={post:.3f}, boosted={post > pre}")
