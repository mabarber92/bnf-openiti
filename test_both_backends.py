"""Quick test of both fuzzy backends."""
import sys
import json

# Test 1: fuzzywuzzy (current default)
print("=" * 70)
print("TEST 1: fuzzywuzzy backend")
print("=" * 70)

import matching.config as cfg
cfg.FUZZY_MATCHER = "fuzzywuzzy"

import subprocess
result1 = subprocess.run([sys.executable, "validate_recall_precision.py"], capture_output=True, text=True)
print(result1.stdout)
if "Recall: 9/10" in result1.stdout:
    print("✓ fuzzywuzzy: 90% recall (baseline)")
    recall1 = 9
else:
    recall1 = int(result1.stdout.split("Recall: ")[1].split("/")[0]) if "Recall:" in result1.stdout else 0
    print(f"fuzzywuzzy recall: {recall1}/10")

print("\n" + "=" * 70)
print("TEST 2: polyfuzz backend")
print("=" * 70)

cfg.FUZZY_MATCHER = "polyfuzz"
result2 = subprocess.run([sys.executable, "validate_recall_precision.py"], capture_output=True, text=True)
print(result2.stdout)
if "Recall:" in result2.stdout:
    recall2 = int(result2.stdout.split("Recall: ")[1].split("/")[0]) if "/" in result2.stdout.split("Recall: ")[1] else 0
    print(f"✓ polyfuzz recall: {recall2}/10")
else:
    recall2 = 0

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"fuzzywuzzy: {recall1}/10")
print(f"polyfuzz:   {recall2}/10")
if recall2 >= 9:
    print("✓ PolyFuzz maintains recall!")
else:
    print("✗ PolyFuzz lost recall - reverting to fuzzywuzzy")
