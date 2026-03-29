from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

_ENV = Path(__file__).resolve().parent / ".env"


def _token() -> str:
    if _ENV.is_file():
        for line in _ENV.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            if s.startswith("#") or "=" not in s:
                continue
            k, _, v = s.partition("=")
            if k.strip() == "TELEGRAM_BOT_TOKEN":
                return v.strip().strip('"').strip("'")
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def _get_updates(token: str, offset: int) -> dict:
    q = urllib.parse.urlencode({"timeout": 30, "offset": offset})
    url = f"https://api.telegram.org/bot{token}/getUpdates?{q}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    token = _token()
    if not token:
        print("Set TELEGRAM_BOT_TOKEN in .env or environment", file=sys.stderr)
        sys.exit(1)

    offset = 0
    print("Add the bot to the group, send any message there. Ctrl+C to exit.")
    while True:
        try:
            data = _get_updates(token, offset)
        except urllib.error.HTTPError as e:
            print(e.read().decode(), file=sys.stderr)
            continue
        except Exception as e:
            print(e, file=sys.stderr)
            continue
        if not data.get("ok"):
            print(json.dumps(data, indent=2))
            continue
        for upd in data.get("result", []):
            offset = upd["update_id"] + 1
            msg = upd.get("message") or upd.get("channel_post")
            if not msg:
                continue
            chat = msg.get("chat") or {}
            cid = chat.get("id")
            ctype = chat.get("type")
            title = chat.get("title") or chat.get("username") or ""
            uname = chat.get("username") or ""
            print(f"TELEGRAM_CHAT_ID={cid}   type={ctype}   title={title!r}   @{uname}")


if __name__ == "__main__":
    main()
