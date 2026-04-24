from matching.normalize import normalize_for_matching

test_cases = [
    ("Ahmad b. Muhammad", "ahmad b. muhammad"),
    ("ʿAbd al-Rahman", "ʿabd al-rahman"),
    ("Cabd al-Rahman", "ʿabd al-rahman"),
    ("cabd al-rahman", "ʿabd al-rahman"),
    ("al-Ṭabarī", "al-tabari"),
    ("al-Maqrīzī", "al-maqrizi"),
    ("Kitab al-Fiqh", "kitab al-fiqh"),
]

with open("test_norm_results.txt", "w", encoding='utf-8') as f:
    f.write("Testing normalization:\n\n")
    all_pass = True
    for input_str, expected in test_cases:
        result = normalize_for_matching(input_str)
        status = "PASS" if result == expected else "FAIL"
        if status == "FAIL":
            all_pass = False
        f.write(f"{status:4} | {input_str:25} -> {result:25} (expected: {expected})\n")
    f.write(f"\nAll tests passed: {all_pass}\n")

print("Results written to test_norm_results.txt")
