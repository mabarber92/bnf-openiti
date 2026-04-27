"""
Analyze parameter sweep results and identify optimal configurations.

Highlights:
- Pareto frontier (configs where precision/recall can't be improved without tradeoff)
- Configurations maintaining ≥90% recall
- Best precision achievers within recall constraint
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))


def calculate_pareto_frontier(df):
    """
    Identify Pareto frontier: configurations where no other config is strictly better.

    A config is on the frontier if there's no other config with both:
    - Higher or equal precision
    - Higher or equal recall
    """
    frontier_mask = np.ones(len(df), dtype=bool)

    for i in range(len(df)):
        for j in range(len(df)):
            if i != j:
                # If config j dominates config i (both metrics >= and at least one >)
                if (df.iloc[j]['precision'] >= df.iloc[i]['precision'] and
                    df.iloc[j]['recall'] >= df.iloc[i]['recall'] and
                    (df.iloc[j]['precision'] > df.iloc[i]['precision'] or
                     df.iloc[j]['recall'] > df.iloc[i]['recall'])):
                    frontier_mask[i] = False
                    break

    return frontier_mask


def main():
    """Analyze sweep results."""
    results_path = Path(__file__).parent / "results" / "sweep_results.csv"

    if not results_path.exists():
        print(f"ERROR: Results file not found: {results_path}")
        print("Run: python sweep_thresholds.py")
        return

    # Load results
    df = pd.read_csv(results_path)

    print("\n" + "=" * 100)
    print("PARAMETER SWEEP ANALYSIS")
    print("=" * 100)

    print(f"\nResults loaded: {len(df)} configurations")
    print(f"Timestamp: {df['timestamp'].iloc[0]}")

    # Filter to recall >= 0.90 (non-negotiable constraint)
    recall_constraint = 0.90
    df_qualified = df[df['recall'] >= recall_constraint].copy()

    print(f"\n--- RECALL CONSTRAINT: ≥{recall_constraint:.0%} ---")
    print(f"Qualifying configurations: {len(df_qualified)} / {len(df)}")

    if len(df_qualified) == 0:
        print("WARNING: No configurations meet recall constraint!")
        print("\nTop 10 by recall:")
        top_recall = df.nlargest(10, 'recall')[
            ['author_threshold', 'title_threshold', 'idf_label', 'precision', 'recall', 'f1']
        ]
        print(top_recall.to_string(index=False))
        return

    # Calculate Pareto frontier within qualified configs
    frontier_mask = calculate_pareto_frontier(df_qualified)
    df_frontier = df_qualified[frontier_mask].copy()

    print(f"Pareto frontier: {len(df_frontier)} configurations")
    print("\n" + "-" * 100)
    print("PARETO FRONTIER (recall ≥90%, non-dominated configs)")
    print("-" * 100)

    # Sort by precision (descending)
    df_frontier_sorted = df_frontier.sort_values('precision', ascending=False)

    print("\n{:<8} {:<8} {:<12} {:<12} {:<12} {:<12} {:<8}".format(
        "Author", "Title", "IDF Type", "Precision", "Recall", "F1", "Better?"
    ))
    print("-" * 100)

    baseline_precision = 0.90
    baseline_recall = 0.90

    for _, row in df_frontier_sorted.iterrows():
        better = ""
        if row['precision'] > baseline_precision and row['recall'] >= baseline_recall:
            better = "✓ BEATS"
        elif row['precision'] >= baseline_precision and row['recall'] >= baseline_recall:
            better = "✓ MATCHES"

        print("{:<8.2f} {:<8.2f} {:<12} {:<12.1%} {:<12.1%} {:<12.3f} {:<8}".format(
            row['author_threshold'],
            row['title_threshold'],
            row['idf_label'],
            row['precision'],
            row['recall'],
            row['f1'],
            better
        ))

    # Find best by precision (within recall constraint)
    best_precision_idx = df_qualified['precision'].idxmax()
    best_precision_row = df_qualified.loc[best_precision_idx]

    print("\n" + "-" * 100)
    print("BEST PRECISION (maintaining ≥90% recall)")
    print("-" * 100)
    print(f"Config: Author={best_precision_row['author_threshold']:.2f}, "
          f"Title={best_precision_row['title_threshold']:.2f}, "
          f"{best_precision_row['idf_label']}")
    print(f"Metrics: Precision={best_precision_row['precision']:.1%}, "
          f"Recall={best_precision_row['recall']:.1%}, "
          f"F1={best_precision_row['f1']:.3f}")
    print(f"Baseline: Precision=90%, Recall=90%")
    if best_precision_row['precision'] > baseline_precision:
        print(f"✓ IMPROVEMENT: +{(best_precision_row['precision'] - baseline_precision):.1%} precision")
    else:
        print(f"⚠ Trade-off: {(baseline_precision - best_precision_row['precision']):.1%} lower precision")

    # Comparison: IDF vs No IDF
    print("\n" + "-" * 100)
    print("IDF EFFECT (averaged across thresholds)")
    print("-" * 100)

    for idf_label in ["No IDF", "IDF^3", "IDF^4"]:
        subset = df_qualified[df_qualified['idf_label'] == idf_label]
        if len(subset) > 0:
            avg_precision = subset['precision'].mean()
            avg_recall = subset['recall'].mean()
            avg_f1 = subset['f1'].mean()
            max_precision = subset['precision'].max()

            print(f"\n{idf_label}:")
            print(f"  Avg Precision: {avg_precision:.1%}")
            print(f"  Avg Recall: {avg_recall:.1%}")
            print(f"  Avg F1: {avg_f1:.3f}")
            print(f"  Max Precision: {max_precision:.1%}")

    # Full qualified results
    print("\n" + "-" * 100)
    print("ALL QUALIFIED CONFIGURATIONS (recall ≥90%, sorted by precision)")
    print("-" * 100)
    df_qualified_sorted = df_qualified.sort_values('precision', ascending=False)
    print("\n{:<8} {:<8} {:<12} {:<12} {:<12} {:<8} {:<8}".format(
        "Author", "Title", "IDF Type", "Precision", "Recall", "F1", "Extra"
    ))
    print("-" * 100)
    for _, row in df_qualified_sorted.iterrows():
        print("{:<8.2f} {:<8.2f} {:<12} {:<12.1%} {:<12.1%} {:<8.3f} {:<8}".format(
            row['author_threshold'],
            row['title_threshold'],
            row['idf_label'],
            row['precision'],
            row['recall'],
            row['f1'],
            int(row['extra_matches'])
        ))

    print("\n" + "=" * 100)
    print("Next: Review configurations and select best parameter set")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
