#!/usr/bin/env python3
"""
CLI for the BNF-OpenITI matching pipeline.

Runs the full three-stage fuzzy matching pipeline:
  Stage 1: Author matching (BNF authors → OpenITI author URIs)
  Stage 2: Title matching (BNF titles → OpenITI book URIs)
  Stage 3: Combined matching (intersection filtering)
  Stage 4: Classification (confidence-based tiering)

Usage:
  python run_matching_pipeline.py --bnf path/to/bnf_parsed.json --sample
  python run_matching_pipeline.py --bnf path/to/bnf_parsed.json --full
  python run_matching_pipeline.py --bnf path/to/bnf_parsed.json --run-id my_test --parallel
"""

import argparse
import json
import sys
from pathlib import Path

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
from matching.config import (
    OPENITI_CORPUS_PATH, BNF_SAMPLE_PATH, BNF_FULL_PATH,
    get_run_dir, get_output_files
)


def aggregate_results(pipeline, run_dir: Path) -> dict:
    """Aggregate pipeline results into classification tiers."""
    summary = {
        "high_confidence": [],
        "author_only": [],
        "title_only": [],
        "unmatched": [],
    }

    for bnf_id in sorted(pipeline.bnf_records.keys()):
        stage3 = pipeline.get_stage3_result(bnf_id) or []
        stage1 = pipeline.get_stage1_result(bnf_id) or []
        stage2 = pipeline.get_stage2_result(bnf_id) or []
        classified = pipeline.get_classification(bnf_id)

        record = {
            "bnf_id": bnf_id,
            "bnf_title": pipeline.bnf_records[bnf_id].title_lat or pipeline.bnf_records[bnf_id].title_ara,
            "matches": stage3,
        }

        if classified == "high_confidence":
            summary["high_confidence"].append(record)
        elif stage1 and stage2:
            summary["author_only"].append(record)
        elif stage2:
            summary["title_only"].append(record)
        else:
            summary["unmatched"].append(record)

    return summary


def write_results(pipeline, run_dir: Path) -> None:
    """Write results to JSON files."""
    run_dir.mkdir(parents=True, exist_ok=True)
    files = get_output_files(run_dir)

    # Aggregate by tier
    summary = aggregate_results(pipeline, run_dir)

    # Write classification tiers
    for tier, records in summary.items():
        with open(files[tier], "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        print(f"  {tier}: {len(records)} records → {files[tier]}")

    # Write summary stats
    with open(files["summary"], "w", encoding="utf-8") as f:
        f.write(f"Matching Pipeline Results\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total BNF records: {len(pipeline.bnf_records)}\n")
        f.write(f"High confidence matches: {len(summary['high_confidence'])}\n")
        f.write(f"Author-only matches: {len(summary['author_only'])}\n")
        f.write(f"Title-only matches: {len(summary['title_only'])}\n")
        f.write(f"Unmatched: {len(summary['unmatched'])}\n\n")
        f.write(f"Output directory: {run_dir}\n")

    # Write manifest (for reproducibility)
    manifest = {
        "run_id": pipeline.run_id,
        "bnf_records_count": len(pipeline.bnf_records),
        "results": {
            "high_confidence": len(summary["high_confidence"]),
            "author_only": len(summary["author_only"]),
            "title_only": len(summary["title_only"]),
            "unmatched": len(summary["unmatched"]),
        },
        "output_files": {k: str(v) for k, v in files.items()},
    }
    with open(files["manifest"], "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Run BNF-OpenITI fuzzy matching pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Run on sample (500 records):
    python run_matching_pipeline.py --sample

  Run on full BNF corpus (7,825 records):
    python run_matching_pipeline.py --full --parallel

  Run custom dataset with specific run ID:
    python run_matching_pipeline.py --bnf /path/to/data.json --run-id my_test
        """,
    )

    # Data selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sample",
        action="store_true",
        help=f"Run on sample set ({BNF_SAMPLE_PATH})",
    )
    group.add_argument(
        "--full",
        action="store_true",
        help=f"Run on full BNF corpus ({BNF_FULL_PATH})",
    )
    group.add_argument(
        "--bnf",
        type=Path,
        help="Path to custom BNF JSON file",
    )

    # Options
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Identifier for this run (default: auto-generated from data source)",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Use parallel processing (ProcessPoolExecutor)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=True,
        help="Print detailed progress (default: True)",
    )
    parser.add_argument(
        "--confidence-filtering",
        action="store_true",
        help="Enable confidence-dependent filtering in Stage 3",
    )

    args = parser.parse_args()

    # Determine BNF path and run ID
    if args.sample:
        bnf_path = BNF_SAMPLE_PATH
        default_run_id = "sample_500"
    elif args.full:
        bnf_path = BNF_FULL_PATH
        default_run_id = "full_7825"
    else:
        bnf_path = args.bnf

    run_id = args.run_id or default_run_id

    print(f"\n{'='*80}")
    print(f"BNF-OpenITI MATCHING PIPELINE")
    print(f"{'='*80}")
    print(f"Run ID: {run_id}")
    print(f"BNF data: {bnf_path}")
    print(f"OpenITI corpus: {OPENITI_CORPUS_PATH}")
    print(f"Parallel processing: {args.parallel}")
    print(f"Confidence filtering: {args.confidence_filtering}")

    # Load data
    print(f"\nLoading data...")
    try:
        bnf_records = load_bnf_records(str(bnf_path))
        openiti_data = load_openiti_corpus(str(OPENITI_CORPUS_PATH))
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  BNF records: {len(bnf_records)}")
    print(f"  OpenITI books: {len(openiti_data['books'])}")
    print(f"  OpenITI authors: {len(openiti_data['authors'])}")

    # Create and configure pipeline
    pipeline = MatchingPipeline(
        bnf_records,
        openiti_data,
        run_id=run_id,
        verbose=args.verbose,
    )

    # Register stages
    pipeline.register_stage(AuthorMatcher(verbose=args.verbose, use_parallel=args.parallel))
    pipeline.register_stage(TitleMatcher(verbose=args.verbose, use_parallel=args.parallel))
    pipeline.register_stage(
        CombinedMatcher(
            verbose=args.verbose,
            use_confidence_filtering=args.confidence_filtering,
        )
    )
    pipeline.register_stage(Classifier(verbose=args.verbose))

    # Run pipeline
    print(f"\n{'='*80}")
    print("RUNNING PIPELINE")
    print(f"{'='*80}")
    pipeline.run()

    # Write results
    print(f"\n{'='*80}")
    print("WRITING RESULTS")
    print(f"{'='*80}")
    run_dir = get_run_dir(run_id)
    write_results(pipeline, run_dir)

    print(f"\n{'='*80}")
    print("PIPELINE COMPLETE")
    print(f"{'='*80}")
    print(f"Results saved to: {run_dir}")
    print(f"View summary: {get_output_files(run_dir)['summary']}")


if __name__ == "__main__":
    main()
