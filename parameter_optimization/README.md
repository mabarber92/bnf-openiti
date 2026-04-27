# Parameter Optimization Suite

Systematically test and optimize matching pipeline parameters.

## Overview

The BNF-OpenITI matching pipeline has several tunable parameters:
- **Author threshold** (0.75–0.95): minimum fuzzy match score for author candidates
- **Title threshold** (0.75–0.95): minimum fuzzy match score for title candidates  
- **IDF weighting**: disable, or enable with penalty exponent 3 (cubic) or 4 (quartic)

This suite sweeps through all reasonable combinations to find the optimal configuration.

## Usage

### 1. Run the Parameter Sweep

```bash
python parameter_optimization/sweep_thresholds.py
```

This will:
- Test 75 configurations (5 author × 5 title × 3 IDF variants)
- Run each configuration through the matching pipeline in parallel (pool of 10)
- Measure Precision, Recall, F1 on the test set (correspondence.json)
- Save results to `parameter_optimization/results/sweep_results.csv`

Typical runtime: ~5-10 minutes depending on hardware.

### 2. Analyze Results

```bash
python parameter_optimization/analyze_results.py
```

This will:
- Load the sweep results
- Filter to configurations maintaining ≥90% recall (hard constraint)
- Identify Pareto frontier (non-dominated configurations)
- Compare IDF vs no-IDF effect
- Print formatted table highlighting best configurations

## Output Format

### sweep_results.csv

| author_threshold | title_threshold | idf_enabled | penalty_exponent | precision | recall | f1 | extra_matches | correct_matches |
|---|---|---|---|---|---|---|---|---|
| 0.75 | 0.75 | False | 3 | 0.456 | 0.900 | 0.607 | 24 | 9 |
| 0.80 | 0.85 | True | 3 | 0.818 | 0.900 | 0.857 | 2 | 9 |
| ... | ... | ... | ... | ... | ... | ... | ... | ... |

### analyze_results.py Output

Shows three sections:
1. **Pareto Frontier**: Non-dominated configurations (can't improve one metric without hurting another)
2. **Best Precision**: Configuration with highest precision while maintaining ≥90% recall
3. **IDF Effect**: Average metrics for No IDF vs IDF^3 vs IDF^4

## Baseline to Beat

Based on initial testing:
- **Precision**: 90%
- **Recall**: 90%

Goal: Find configuration that matches or beats this on the test set.

## Notes

- Uses existing test set (correspondence.json, 12 records)
- All pipeline stages run **sequentially** (use_parallel=False) to avoid nested parallelization
- Parameter combinations run in **parallel** (pool of 10 workers) for speed
- Each sweep takes measurements but doesn't recalculate IDF weights between thresholds—only sweeps threshold values
