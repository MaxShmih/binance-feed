from __future__ import annotations

import os
import sys
from typing import Iterable

import httpx

TG_MAX = 4096


def _chat_ids() -> list[int]:
    raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out


def send_telegram(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chats = _chat_ids()
    if not token or not chats:
        print(
            "Telegram: skipped (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID).",
            file=sys.stderr,
        )
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok = True
    for chunk in _chunks(text, TG_MAX):
        for chat_id in chats:
            try:
                r = httpx.post(
                    url,
                    json={
                        "chat_id": chat_id,
                        "text": chunk,
                        "disable_web_page_preview": True,
                    },
                    timeout=30.0,
                )
                if r.status_code != 200:
                    try:
                        detail = r.text.strip()
                    except Exception:
                        detail = ""
                    print(
                        f"Telegram: sendMessage failed (status={r.status_code}, chat_id={chat_id})"
                        + (f": {detail[:400]}" if detail else ""),
                        file=sys.stderr,
                    )
                    ok = False
            except httpx.HTTPError:
                print(
                    f"Telegram: sendMessage HTTP error (chat_id={chat_id}).",
                    file=sys.stderr,
                )
                ok = False
    return ok


def _chunks(s: str, size: int) -> Iterable[str]:
    if len(s) <= size:
        yield s
        return
    for i in range(0, len(s), size):
        yield s[i : i + size]
