import re

from constants import VOLUME_MAP


def remove_duplicate_words(text: str) -> str:
    """
    Remove duplicate words from text while preserving order.

    Args:
        text: Input text

    Returns:
        Text with duplicate words removed
    """
    if not text:
        return ''

    words = text.split()
    seen_words = set()
    unique_words = []

    for word in words:
        word_lower = word.lower()
        if word_lower not in seen_words:
            seen_words.add(word_lower)
            unique_words.append(word)

    return ' '.join(unique_words)


def normalize_viscosity_grades(text: str) -> str:
    """
    Remove hyphens from viscosity grades (e.g., "75W-80" -> "75W80", "5W-40" -> "5W40").

    Args:
        text: Input text

    Returns:
        Text with viscosity grade hyphens removed
    """
    if not text:
        return ''

    normalized = re.sub(r'(\d+w)-(\d+)', r'\1\2', text, flags=re.IGNORECASE)

    return normalized


def parse_volume_from_string(text: str) -> tuple[str, str, str]:
    """
    Parse volume information from product name string.

    Args:
        text: Product name string

    Returns:
        Tuple of (cleaned_name, volume_number, volume_unit)
        - cleaned_name: Name with volume removed and cleaned
        - volume_number: Extracted volume number (e.g., "1", "500", "4.5")
        - volume_unit: Extracted volume unit (e.g., "l", "ml", "kg")
    """
    if not text:
        return '', '', ''

    text = str(text).strip()
    volume_number = ''
    volume_unit = ''


    pattern = r'(\d+(?:\.\d+)?)\s*(л|мл|кг|г)\.?\s*$'
    
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        volume_number = match.group(1)
        volume_unit_raw = match.group(2).lower()
        if volume_unit_raw in VOLUME_MAP:
            volume_unit = VOLUME_MAP[volume_unit_raw]
            text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()

    return text, volume_number, volume_unit


def remove_russian_characters(text: str) -> str:
    return re.sub(r'[а-яё]', '', text, flags=re.IGNORECASE).strip()


def normalize_product_name(name: str) -> str:
    if not name:
        return ''
    name = str(name).strip()

    # Remove digits from the end (for cases like "866904")
    name = re.sub(r'\s+\d+\s*$', '', name)

    name = re.sub(r'^,\s*|,\s*$', '', name)
    name = re.sub(r'\(|\)', ' ', name)

    normalized = re.sub(r'\band\b', '&', name, flags=re.IGNORECASE)
    normalized = normalized.lower()
    normalized = re.sub(r'[.,]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)

    normalized = remove_duplicate_words(normalized)

    return normalized
