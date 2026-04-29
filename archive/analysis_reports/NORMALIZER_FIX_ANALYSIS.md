# Normalizer Issues and Proposed Fixes

## Current Problems

### 1. **Missing Uppercase Variants in Conversion Table**

The conversion table maps lowercase `š` → `sh`, but BNF data contains uppercase `Š`.

**Evidence:**
```
Original:  al-Šarif
Step 1:    al-Šarif    (unchanged, not in _apply_openiti_conversions)
Step 2:    al-arif     (Š REMOVED, not in conversion table because only š is mapped)
Expected:  al-sharif   (should map Š → sh)
```

**Root cause:** The conversion table only includes lowercase letters. When `normalize_with_diacritics()` processes uppercase Š (U+0160), it's not found in the mappings dict, so it falls through to the "unmapped non-ASCII" case and gets removed.

**Fix:** Add uppercase variants to conversion table:
```
Š (U+0160) → sh
À (U+00C0) → a
É (U+00C9) → e
```

### 2. **Missing Accented Vowels in Conversion Table**

BNF data contains French/European diacritics (â, ê, ô, etc.) that aren't in the table.

**Evidence:**
```
Original:  Allâh
Step 1:    Allâh      (unchanged)
Step 2:    allh       (â REMOVED, not in table)
Expected:  allah      (should map â → a)
```

**Missing mappings:**
- â (U+00E2) → a
- ê (U+00EA) → e
- ô (U+00F4) → o
- û (U+00FB) → u
- è (U+00E8) already in table ✓
- é (U+00E9) already in table ✓

### 3. **Default Normalizer Strips ʿ (Ayn)**

The legacy `normalize_transliteration()` removes combining diacritical marks via NFD/NFC, which affects ayn.

**Current behavior:**
- With conversion table ON: `ʿ` preserved (mapped in table as `ʿ` → `ʿ`)
- With conversion table OFF: `ʿ` stripped (not in ASCII-only fallback)

**This is wrong:** Both paths should treat ayn consistently. The NFD/NFC approach in `normalize_transliteration()` should preserve `ʿ` since it's a distinct character, not a combining mark.

## Proposed Fixes (Step-by-Step)

### Fix 1: Update Conversion Table with Missing Characters

**File:** `outputs/bnf_survey/diacritic_conversions.csv`

Add these rows:
```
Š,U+0160,LATIN CAPITAL LETTER S WITH CARON,Lu,sh,Uppercase variant of š
À,U+00C0,LATIN CAPITAL LETTER A WITH GRAVE,Lu,a,French grave accent
Á,U+00C1,LATIN CAPITAL LETTER A WITH ACUTE,Lu,a,French acute accent
Â,U+00C2,LATIN CAPITAL LETTER A WITH CIRCUMFLEX,Lu,a,French circumflex
Ä,U+00C4,LATIN CAPITAL LETTER A WITH DIAERESIS,Lu,a,French diaeresis
È,U+00C8,LATIN CAPITAL LETTER E WITH GRAVE,Lu,e,French grave accent
É,U+00C9,LATIN CAPITAL LETTER E WITH ACUTE,Lu,e,French acute accent
Ê,U+00CA,LATIN CAPITAL LETTER E WITH CIRCUMFLEX,Lu,e,French circumflex
Ë,U+00CB,LATIN CAPITAL LETTER E WITH DIAERESIS,Lu,e,French diaeresis
Ì,U+00CC,LATIN CAPITAL LETTER I WITH GRAVE,Lu,i,Grave accent
Í,U+00CD,LATIN CAPITAL LETTER I WITH ACUTE,Lu,i,Acute accent
Î,U+00CE,LATIN CAPITAL LETTER I WITH CIRCUMFLEX,Lu,i,Circumflex
Ï,U+00CF,LATIN CAPITAL LETTER I WITH DIAERESIS,Lu,i,Diaeresis
Ò,U+00D2,LATIN CAPITAL LETTER O WITH GRAVE,Lu,o,French grave accent
Ó,U+00D3,LATIN CAPITAL LETTER O WITH ACUTE,Lu,o,French acute accent
Ô,U+00D4,LATIN CAPITAL LETTER O WITH CIRCUMFLEX,Lu,o,French circumflex
Ö,U+00D6,LATIN CAPITAL LETTER O WITH DIAERESIS,Lu,o,French diaeresis
Ù,U+00D9,LATIN CAPITAL LETTER U WITH GRAVE,Lu,u,French grave accent
Ú,U+00DA,LATIN CAPITAL LETTER U WITH ACUTE,Lu,u,French acute accent
Û,U+00DB,LATIN CAPITAL LETTER U WITH CIRCUMFLEX,Lu,u,Circumflex
Ü,U+00DC,LATIN CAPITAL LETTER U WITH DIAERESIS,Lu,u,French diaeresis
â,U+00E2,LATIN SMALL LETTER A WITH CIRCUMFLEX,Ll,a,French circumflex
ê,U+00EA,LATIN SMALL LETTER E WITH CIRCUMFLEX,Ll,e,French circumflex
î,U+00EE,LATIN SMALL LETTER I WITH CIRCUMFLEX,Ll,i,French circumflex
ô,U+00F4,LATIN SMALL LETTER O WITH CIRCUMFLEX,Ll,o,French circumflex
û,U+00FB,LATIN SMALL LETTER U WITH CIRCUMFLEX,Ll,u,French circumflex
ä,U+00E4,LATIN SMALL LETTER A WITH DIAERESIS,Ll,a,French diaeresis
ë,U+00EB,LATIN SMALL LETTER E WITH DIAERESIS,Ll,e,French diaeresis
ï,U+00EF,LATIN SMALL LETTER I WITH DIAERESIS,Ll,i,French diaeresis
ö,U+00F6,LATIN SMALL LETTER O WITH DIAERESIS,Ll,o,French diaeresis
ü,U+00FC,LATIN SMALL LETTER U WITH DIAERESIS,Ll,u,French diaeresis
```

### Fix 2: Ensure normalize_transliteration() Preserves ʿ

**File:** `matching/normalize.py`, function `normalize_transliteration()`

Currently it does NFD decomposition which might affect ayn. We should:
1. Explicitly preserve `ʿ` before NFD
2. Restore it after diacritic removal

**Change:**
```python
def normalize_transliteration(text: str) -> str:
    if not text:
        return ""

    # 1. Convert backtick to ʿ (BetaCode ayn representation)
    text = text.replace("`", "ʿ")
    
    # PRESERVE AYN BEFORE DECOMPOSITION
    ayn_preserved = "ʿ" in text

    # 2. Remove diacritical marks (macrons, underscores, etc.)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = unicodedata.normalize("NFC", text)
    
    # 3. Lowercase
    text = text.lower()

    # 4. Normalize whitespace and hyphens
    text = re.sub(r"-+", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    return text
```

(Note: Actually ayn might already be preserved—need to test this)

### Fix 3: Test Both Normalizers Produce Identical Results

Once the conversion table is fixed, both code paths (table ON/OFF) should produce the same output because:
- Table ON: Uses conversion table to map Š→sh, â→a
- Table OFF: Falls back to ASCII-only, but without any mappings, non-ASCII gets stripped

We need them to be **equivalent**, not just "both broken differently."

## Validation Steps

After applying fixes:

1. Run `debug_normalizer_direct.py` and verify:
   - `al-Šarif` → `al-sharif` (not `al-arif`)
   - `Allâh` → `allah` (not `allh`)
   - Results are identical with table ON and OFF

2. Re-run parameter sweep on 11-record set with both table settings

3. Verify results match or improve on original 90% precision baseline

## Summary

| Issue | Current | Proposed |
|-------|---------|----------|
| Uppercase Š in table | ✗ Missing | ✓ Add U+0160 → sh |
| Accented vowels | ✗ Missing â, ê, ô, û | ✓ Add French vowels |
| ʿ handling consistency | ✗ Asymmetric (ON≠OFF) | ✓ Consistent both ways |
| Data loss | ✗ al-Šarif→al-arif | ✓ al-Šarif→al-sharif |
