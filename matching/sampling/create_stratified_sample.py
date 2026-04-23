"""
Create a stratified random sample of BNF records for matching pipeline validation.

Stratification dimensions:
1. Data completeness: both author+title, author-only, title-only, sparse
2. Manuscript type: single-author vs. composite
3. Subject diversity: historiography, religious, sciences, language, other
4. Time period: early (<900 AH), medieval (900-1200 AH), late (>1200 AH)
5. Metadata richness: rich, moderate, sparse

Filters out Quran copies (crude filter on subject + title) to ensure diversity.
Saves reduced BNF JSON for reproducible testing.
"""

import json
import random
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

# Configuration
SAMPLE_SIZE = 500
QURAN_KEYWORDS = {
    'quran', 'qur\'an', "qur'an", 'quran al-karim', 'القرآن',
    'furqan', 'koran', 'coran'  # Various transliterations and aliases
}
OUTPUT_PATH = Path("matching/sampling")
SAMPLE_JSON = OUTPUT_PATH / "bnf_sample_500.json"

def is_quran_record(record: dict) -> bool:
    """Filter Quran copies via title fields (well-documented there)."""
    # Check titles (Latin and Arabic) - Qurans are well-documented here
    for title in record.get('title_lat', []):
        if any(kw in title.lower() for kw in QURAN_KEYWORDS):
            return True

    for title in record.get('title_ara', []):
        # Arabic Quran keywords
        if 'القرآن' in title or 'الفرقان' in title:
            return True

    return False


def classify_data_completeness(record: dict) -> str:
    """Classify record by author+title data availability."""
    has_creator = bool(record.get('creator_lat')) or bool(record.get('creator_ara'))
    has_title = bool(record.get('title_lat')) or bool(record.get('title_ara'))

    if has_creator and has_title:
        return 'both'
    elif has_creator:
        return 'author_only'
    elif has_title:
        return 'title_only'
    else:
        return 'sparse'


def classify_subject(record: dict) -> str:
    """Classify record by subject domain."""
    subjects = set(s.lower() for s in record.get('subject', []))

    hist_keywords = {'histoire', 'histoire.', 'biography', 'biographie', 'généalogie', 'genealogy', 'tabaqat'}
    religious = {'qur', 'coran', 'hadith', 'tafsir', 'fiqh', 'droit', 'théologie', 'jurisprudence', 'sharia', 'shariah'}
    sciences = {'science', 'astronomie', 'médecin', 'medicine', 'mathématique', 'mathematics', 'géographie', 'geography'}
    language = {'langue', 'language', 'grammaire', 'grammar', 'linguistique', 'littérature', 'literature', 'poésie', 'poetry'}

    if any(kw in subj for subj in subjects for kw in hist_keywords):
        return 'historiography'
    elif any(kw in subj for subj in subjects for kw in religious):
        return 'religious'
    elif any(kw in subj for subj in subjects for kw in sciences):
        return 'sciences'
    elif any(kw in subj for subj in subjects for kw in language):
        return 'language'
    else:
        return 'other'


def classify_time_period(record: dict) -> str:
    """Classify by copy date."""
    date_from = record.get('date_from')

    if date_from is None:
        return 'unknown'
    elif date_from < 900:
        return 'early'
    elif date_from < 1200:
        return 'medieval'
    else:
        return 'late'


def classify_manuscript_type(record: dict) -> str:
    """Classify by composite status."""
    return 'composite' if record.get('is_composite') else 'single_author'


def estimate_richness(record: dict) -> str:
    """Estimate metadata richness from field counts."""
    creator_count = len(record.get('creator_lat', [])) + len(record.get('creator_ara', []))
    title_count = len(record.get('title_lat', [])) + len(record.get('title_ara', []))
    desc_count = len(record.get('description_lat', []))

    # Rich: multiple creators/titles or long descriptions
    if creator_count >= 3 or title_count >= 2 or desc_count >= 5:
        return 'rich'
    elif creator_count >= 1 or title_count >= 1 or desc_count >= 2:
        return 'moderate'
    else:
        return 'sparse'


def load_bnf_corpus(path: Path) -> Dict:
    """Load BNF parsed JSON."""
    print(f"Loading BNF corpus from {path}...")
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data['records']


def create_sample(records: Dict, sample_size: int = 500) -> Tuple[List[str], Dict]:
    """
    Create stratified sample of BNF record IDs.

    Returns:
        (sample_ids, stratification_stats)
    """
    print(f"Processing {len(records)} records for stratification...")

    # Build stratification bins
    bins = defaultdict(list)
    quran_count = 0

    for bid, record in records.items():
        if is_quran_record(record):
            quran_count += 1
            continue

        # Classify across all dimensions
        completeness = classify_data_completeness(record)
        subject = classify_subject(record)
        time_period = classify_time_period(record)
        mss_type = classify_manuscript_type(record)
        richness = estimate_richness(record)

        # Create bin key
        bin_key = (completeness, subject, time_period, mss_type, richness)
        bins[bin_key].append(bid)

    print(f"Quran records filtered: {quran_count}")
    print(f"Non-Quran records: {len(records) - quran_count}")
    print(f"Total stratification bins: {len(bins)}")

    # Sample proportionally from each bin
    sample_ids = []
    for bin_key, record_ids in bins.items():
        # Calculate proportional allocation
        proportion = len(record_ids) / (len(records) - quran_count)
        bin_target = max(1, round(proportion * sample_size))

        # Sample with replacement if bin is too small
        sampled = random.sample(record_ids, min(bin_target, len(record_ids)))
        sample_ids.extend(sampled)

    # Adjust to exact sample size if needed
    if len(sample_ids) > sample_size:
        sample_ids = random.sample(sample_ids, sample_size)
    elif len(sample_ids) < sample_size:
        # Add more from largest bins
        all_non_quran = [bid for bid, rec in records.items() if not is_quran_record(rec)]
        remaining = set(all_non_quran) - set(sample_ids)
        top_up = random.sample(list(remaining), sample_size - len(sample_ids))
        sample_ids.extend(top_up)

    # Gather stratification stats
    stats = {
        'total_records': len(records),
        'quran_filtered': quran_count,
        'sample_size': len(sample_ids),
        'bins_used': len(bins),
    }

    return sample_ids, stats


def save_sample(records: Dict, sample_ids: List[str], output_path: Path) -> None:
    """Save sampled records to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sample_records = {bid: records[bid] for bid in sample_ids}

    sample_data = {
        '_meta': {
            'schema_version': 1,
            'original_corpus_size': len(records),
            'sample_size': len(sample_ids),
            'generation_method': 'stratified_random_sampling',
            'excluded_quran': True,
        },
        'records': sample_records
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, ensure_ascii=False, indent=2)

    print(f"Saved sample to {output_path}")


def main():
    """Create and save stratified sample."""
    random.seed(42)  # Reproducible

    records = load_bnf_corpus(Path("outputs/bnf_parsed.json"))
    sample_ids, stats = create_sample(records, SAMPLE_SIZE)
    save_sample(records, sample_ids, SAMPLE_JSON)

    print("\n" + "="*60)
    print("STRATIFIED SAMPLE CREATED")
    print("="*60)
    print(f"Original corpus: {stats['total_records']} records")
    print(f"Quran records filtered: {stats['quran_filtered']}")
    print(f"Sample size: {stats['sample_size']}")
    print(f"Stratification bins: {stats['bins_used']}")
    print(f"Output: {SAMPLE_JSON}")
    print("="*60)


if __name__ == "__main__":
    main()
