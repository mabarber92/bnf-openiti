# Matching Development Archive

Trial and error process for surface-form fuzzy matching pipeline.

## Chronological Summary

1. **Initial fuzzy matching test** (`test_fuzzy_transliteration.py`)
   - Tested known correspondences against fuzzy matching
   - Found only 4 actual pairs tested due to field mismatch
   - Revealed need for comprehensive field matching

2. **Greedy search test** (`test_fuzzy_greedy_search.py`)
   - Searched each BNF record against full OpenITI corpus (9,500+ books)
   - Found critical bug: threshold comparison was 0-1 scale vs 0-100 fuzzy scores
   - After fix: 80% recall but terrible precision (88-97% false positives)
   - Conclusion: Raw fuzzy matching on titles insufficient

3. **Transliteration normalization** (`test_fuzzy_greedy_normalized.py`)
   - Applied normalization: ayn variants, diacritics, case, whitespace
   - Improvement: 90% recall maintained, false positives reduced slightly
   - Conclusion: Normalization helps but not enough alone

4. **OpenITI data structure issues discovered**
   - Author name fields mislabeled as `*_ar` when they contained Latin script
   - Should be `*_lat` to match convention
   - Fixed via find-and-replace in JSON

5. **First author matching attempt** (`test_fuzzy_with_author.py`)
   - Separated title and author matching signals
   - Found author-only matching very weak (30% recall, 2-3% precision)
   - Combined (author+title together) showed promise but incomplete

6. **Comprehensive author field extraction** (`test_fuzzy_with_author_comprehensive.py`)
   - Extracted ALL author name variants from OpenITI (slug, Wikidata labels/aliases, all name components)
   - Included BNF description_candidates in author matching
   - Results showed massive improvement in combined signal

7. **Threshold tuning analysis** (`analyze_threshold_combinations.py`)
   - Tested 25 threshold combinations (title × author)
   - Key finding: looser title threshold + stricter author threshold optimal
   - Best balance: `title=0.80 author=0.80` → 90% recall, 52.9% precision, F1=0.667
   - Best precision: `title=0.85 author=0.80` → 60% recall, 100% precision, F1=0.750

## 8. **Parser refactoring iteration** (2026-04-23)
   - Discovered Daraqutni book title contains embedded genealogies separated by `::` and `¶`
   - False positives (5 of 8) match Daraqutni because its verbose metadata matches unrelated BNF records
   - Solution: Refactored OpenITI parser to:
     - Add ArabicBetaCode conversion for author name components (_lat and _ara fields)
     - Split TSV metadata on `::` and `¶` separators to break verbose genealogies
     - Proper field naming convention (name_*_lat, name_*_ara instead of name_*_ar)
   - Rebuilt full corpus with structural improvements
   - Ready to test matching pipeline with reduced false positives

## Key Learnings

- **Signal strength ordering:** Author+Title combined > Title-only >> Author-only
- **Field completeness matters:** Using all available author name variants critical
- **Description fields important:** Author info often appears in description_candidates, especially for composites
- **Threshold tuning trade-off:** Can't have both 90% recall AND 100% precision; architectural decisions needed
- **CSV encoding issues:** Using csv.DictWriter creates newlines in fields; switch to pandas
- **Verbose metadata problem:** Some OpenITI titles contain embedded genealogies/author data that causes false matches; splitting on separators is necessary
- **False positive disambiguation:** Not all false positives are errors—some reveal genuine historical relationships between manuscripts

## Files in This Archive

- `test_fuzzy_*.py` - Various greedy search test iterations
- `test_threshold_tuning.py` - Earlier threshold testing approach (superseded by post-processing)
- Signal diagnosis CSVs and reports from comprehensive tests
- `matching_results_author/title.csv` - Pre-refactor matching results (used for false positive analysis)
