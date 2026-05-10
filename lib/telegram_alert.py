#!/usr/bin/env python3
"""Telegram alerting for critical bot events."""
import os
import logging
import aiohttp

logger = logging.getLogger(__name__)

_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")


async def send_alert(message: str) -> None:
    """Send a Telegram message (async). Silently skips if credentials are not configured."""
    if not _BOT_TOKEN or not _CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                url,
                json={"chat_id": _CHAT_ID, "text": message, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=5),
            )
    except Exception as e:
        logger.warning(f"Telegram alert failed: {e}")


def send_alert_sync(message: str) -> None:
    """Send a Telegram message (sync, for use from threads/sync code)."""
    if not _BOT_TOKEN or not _CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    try:
        import requests
        requests.post(
            url,
            json={"chat_id": _CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        logger.warning(f"Telegram alert (sync) failed: {e}")
