import re

def clean_string(text: str, mode: str) -> str:
    if mode == 'alphanumeric':
        return re.sub(r'[^a-zA-Z0-9\s]', '', text).strip()
    if mode == 'alphabetic':
        return re.sub(r'[^a-zA-Z\s]', '', text).strip()
    raise ValueError(f"Unknown mode: {mode}")

def normalize_full_name(name: str, last_name: str) -> tuple[str, str]:
    first = clean_string(name, 'alphabetic')[:25]
    last = clean_string(last_name, 'alphabetic')[:25]
    return first, last
