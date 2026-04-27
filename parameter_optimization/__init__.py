"""
Parameter optimization suite for BNF-OpenITI matching pipeline.

Tests various threshold and IDF weighting configurations to find optimal parameters
for balancing precision and recall while maintaining ≥90% recall.

Usage:
  python sweep_thresholds.py    # Run 75 configurations in parallel
  python analyze_results.py     # Analyze results and identify optimal configs
"""
