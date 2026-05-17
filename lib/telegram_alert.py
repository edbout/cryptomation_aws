#!/usr/bin/env python3
"""Telegram alerting for critical bot events."""
import os
import logging
import aiohttp

logger = logging.getLogger(__name__)

from config import Config

_BOT_TOKEN = Config.TELEGRAM_BOT_TOKEN
_CHAT_ID   = Config.TELEGRAM_CHAT_ID


async def send_alert(message: str) -> None:
    if not _BOT_TOKEN or not _CHAT_ID:
        logger.warning("✗ Telegram skipped: missing token or chat_id")
        return
    url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    try:
        logger.debug(f"📤 Sending Telegram: {message[:50]}...")
        async with aiohttp.ClientSession() as session:
            # First attempt: send with HTML parse mode
            async with session.post(
                url,
                json={"chat_id": _CHAT_ID, "text": message, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                text = await resp.text()
                logger.debug(f"📨 Telegram resp: {resp.status} {text[:200]}")
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if not data.get("ok"):
                        logger.error(f"Telegram API error: {data}")
                    return
                # Fallback: if Telegram rejected due to HTML parse failure, retry as plain text
                if resp.status == 400 and "parse entities" in text.lower():
                    logger.warning(f"✗ Telegram HTML parse error — retrying as plain text: {text[:120]}")
                    async with session.post(
                        url,
                        json={"chat_id": _CHAT_ID, "text": message},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp2:
                        text2 = await resp2.text()
                        if resp2.status != 200:
                            logger.error(f"✗ Telegram plain-text fallback failed {resp2.status}: {text2}")
                        return
                logger.error(f"✗ Telegram failed {resp.status}: {text}")
    except Exception as e:
        logger.error(f"✗ Telegram network error: {e}")

def send_alert_sync(message: str) -> None:
    """Send a Telegram message (sync, for use from threads/sync code)."""
    if not _BOT_TOKEN or not _CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    try:
        import requests
        logger.debug(f"📤 Sending Telegram: {message[:50]}...")
        resp = requests.post(
            url,
            json={"chat_id": _CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=5,
        )
        # Fallback: retry as plain text if HTML parse fails
        if resp.status_code == 400 and "parse entities" in resp.text.lower():
            logger.warning(f"✗ Telegram HTML parse error (sync) — retrying as plain text")
            requests.post(
                url,
                json={"chat_id": _CHAT_ID, "text": message},
                timeout=5,
            )
    except Exception as e:
        logger.warning(f"✗ Telegram alert (sync) failed: {e}")
