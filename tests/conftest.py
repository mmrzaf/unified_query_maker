import re


def squash_ws(s: str) -> str:
    """Normalize whitespace for stable string comparisons."""
    return re.sub(r"\s+", " ", s).strip()
