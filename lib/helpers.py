from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def safe_float(val, default=0.5):
    """Safely convert str/int/float to float from API responses."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            pass
    return default


def get_utc_now() -> datetime:
    """Return current timezone-aware UTC datetime."""
    return datetime.now(ZoneInfo("UTC"))


def get_seconds_since_5m_start(now: datetime | None = None) -> int:
    """Seconds elapsed since the current 5-minute window started."""
    now = now or get_utc_now()
    return (now.minute % 5) * 60 + now.second


def get_current_5m_bar_ts(secs: float) -> int:
    """Snap to UTC 5-minute boundaries: 00, 05, 10, 15..."""
    utc_secs = int(secs)
    return (utc_secs // 300) * 300


def get_current_5m_bar_start(secs: float) -> datetime:
    """Same as above but as timezone-aware datetime (for logging)."""
    return datetime.fromtimestamp(get_current_5m_bar_ts(secs), tz=timezone.utc)


def normalize_asset(asset: str) -> str:
    """Add 'T' suffix if asset doesn't end with USDT (Bybit → Polymarket format)."""
    if not asset.endswith("USDT"):
        return asset + "T"
    return asset


def normalize_polymarket_asset(asset: str) -> str:
    """
    Normalize asset symbol for Polymarket format.

    - Lowercase input → uppercase output
    - Strips "usdt"/"usd" suffixes
    - Removes common punctuation (./-)
    - Strips whitespace
    """
    if not asset:
        return ""

    normalized = (
        str(asset)
        .lower()
        .replace("usdt", "")
        .replace("usd", "")
        .replace(".", "")
        .replace("/", "")
        .replace("-", "")
        .strip()
    )

    return normalized