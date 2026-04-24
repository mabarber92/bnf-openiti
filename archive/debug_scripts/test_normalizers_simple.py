from matching.normalize import normalize_transliteration
from matching.normalize_diacritics import normalize_with_diacritics

# Test strings with different ayn variants
test_cases = [
    "Ahmad b. Muhammad",  # ASCII - should work fine
    "ʿAbd al-Rahman",     # U+02BE ayn
    "Cabd al-Rahman",     # C variant
    "cabd al-rahman",     # c variant  
    "al-Ṭabarī",          # T with dot below
    "al-Tabarī",          # regular i with macron
    "Kitab al-Fiqh",      # Simple case
]

with open("test_output.txt", "w", encoding='utf-8') as f:
    f.write("Testing normalization differences:\n\n")
    
    for text in test_cases:
        legacy = normalize_transliteration(text)
        new = normalize_with_diacritics(text, use_table=True)
        match = "SAME" if legacy == new else "DIFFER"
        f.write(f"{text}\n")
        f.write(f"  Legacy: {repr(legacy)}\n")
        f.write(f"  New:    {repr(new)}\n")
        f.write(f"  Status: {match}\n\n")

print("Results written to test_output.txt")
