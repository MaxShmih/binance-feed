from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")

from telegram_client import send_telegram

SQUARE_ADD_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
TARGET_SPOT_PAIR = "NEARUSDT"
TARGET_PAIR_DISPLAY = "NEAR/USDT"

SYSTEM_NEAR_FEED_PROMPT = """Act as a crypto market analyst writing a single Binance Feed post.

Task:
Write 1 short, engaging post about NEAR Protocol ($NEAR) in English.

Requirements:
- Ground the post in the DATA SNAPSHOT from the user message (Binance spot NEAR/USDT, live 24h stats). If the snapshot is unavailable, use only logical structure (range, consolidation, breakout potential) — do NOT invent specific prices or fake news.
- When you mention prices, write them like a human trader: short decimals only (e.g. 1.19 USDT, 1.149 USDT) — NEVER paste long machine strings like 1.19200000 or 9.1234567.
- Do NOT sound like spam or aggressive promotion.
- Avoid direct calls like "buy now" or "don't miss this".
- Make it feel like a natural observation from a trader.

Style:
- 2 short paragraphs maximum (body text before hashtags).
- Each run you will get a UNIQUE STYLE BRIEF in the user message — follow it strictly so the post does not read like copy-paste from previous posts.
- Vary openings: do NOT default to the same template (avoid repetitive starts like "NEAR Protocol has been", "Looking at the chart", "In the last 24 hours" every time).
- Clean, confident tone. No cringe, no overhype, no fake news. Slight FOMO allowed, subtle.

Structure:
1) Hook (must match the STYLE BRIEF)
2) Insight (structure, behavior, what to watch)

Ending:
- Mention $NEAR naturally in the body (not only in hashtags).
- After the paragraphs, add ONE blank line, then hashtags on their own line(s).

Hashtags (mandatory):
- Always include: $NEAR and #near (Cashtag + hashtag as specified).
- Add 3–5 more relevant tags (e.g. #crypto #altcoins #trading #web3 #DeFi).

Goal:
Make the reader think: "this is interesting, I should keep an eye on NEAR"
NOT: "this is an ad"

Important:
Write like a real trader, not a promoter. Not financial advice.

Output rules:
Reply with ONLY the full post text ready to publish — no title, no markdown code fences, no "Here is your post" or preamble — exactly what goes into the feed."""

STYLE_BRIEFS: tuple[str, ...] = (
    "STYLE: Open with one sharp thesis sentence (no 'Recently', no 'In the last 24h'). Then support it. Dry, confident.",
    "STYLE: Start with a direct question to the reader, then answer in the same flow. Conversational but professional.",
    "STYLE: Lead with volume/liquidity angle first; mention price levels second. Short, punchy sentences.",
    "STYLE: Calm desk notes: understated, 'memo to self' tone. No hype adjectives.",
    "STYLE: Second person — address 'you' as a trader: what to watch on NEAR/USDT this session.",
    "STYLE: One vivid but tasteful metaphor (not childish) for the tape; numbers still grounded in the snapshot.",
    "STYLE: Contrarian hook: acknowledge the obvious move, then add a constructive 'what would change my mind' angle.",
    "STYLE: Macro line (one clause on L1/execution narrative) then snap to spot data from the snapshot only.",
    "STYLE: Twitter-density: tight wording, minimal filler, high signal. Still two proper paragraphs.",
    "STYLE: Open with the exact 24h % change as the first clause (must match snapshot); unexpected second sentence.",
    "STYLE: Story beat: 'Noticed something on the tape…' — observational fiction framing OK; all figures must match snapshot.",
    "STYLE: Paragraph 2 uses three parallel short beats (rhythm like: clause; clause; clause). No bullet characters.",
    "STYLE: Slightly informal voice (still professional): one mild idiom allowed, not slang-heavy.",
    "STYLE: Technician voice: focus on range, invalidation, and what breaks the setup — neutral wording.",
    "STYLE: Begin with the high/low range from the snapshot as the hook, then interpret participation (volume).",
    "STYLE: Skeptical bull: respectful doubt + what would confirm strength. No bearish trash talk.",
    "STYLE: First paragraph only setup; second paragraph only 'what I'm watching next' — clear handoff between them.",
    "STYLE: No emoji anywhere. No exclamation marks in the body paragraphs.",
)

_LONG_DECIMAL = re.compile(r"\b\d+\.\d{5,}\b")


def _to_decimal(v: Any) -> Decimal | None:
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _fmt_price_usdt(v: Any) -> str:
    d = _to_decimal(v)
    if d is None:
        return str(v)
    q = d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    s = format(q, "f").rstrip("0").rstrip(".")
    return s or "0"


def _fmt_percent(v: Any) -> str:
    d = _to_decimal(v)
    if d is None:
        return str(v)
    q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = format(q, "f").rstrip("0").rstrip(".")
    return s or "0"


def _fmt_quote_volume(v: Any) -> str:
    d = _to_decimal(v)
    if d is None:
        return str(v)
    if d >= Decimal("1000000"):
        x = (d / Decimal("1000000")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return f"{format(x, 'f').rstrip('0').rstrip('.')}M"
    if d >= Decimal("1000"):
        x = (d / Decimal("1000")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return f"{format(x, 'f').rstrip('0').rstrip('.')}K"
    q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(q, "f").rstrip("0").rstrip(".")


def humanize_long_decimals(text: str) -> str:
    def repl(m: re.Match[str]) -> str:
        d = _to_decimal(m.group(0))
        if d is None:
            return m.group(0)
        q = d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        s = format(q, "f").rstrip("0").rstrip(".")
        return s or "0"

    return _LONG_DECIMAL.sub(repl, text)


def append_spot_trade_link(body: str) -> str:
    url = os.environ.get("SQUARE_SPOT_TRADE_URL", "").strip()
    if not url:
        return body
    cta = os.environ.get(
        "SQUARE_TRADE_CTA_LINE",
        "Spot NEAR/USDT (your ref link):",
    ).strip()
    return f"{body.rstrip()}\n\n{cta}\n{url}"


def finalize_post_body(body: str) -> str:
    body = humanize_long_decimals(body)
    body = append_spot_trade_link(body)
    return body


def load_square_content_extra() -> dict[str, Any]:
    raw = os.environ.get("SQUARE_CONTENT_EXTRA", "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(obj, dict):
        return {}
    return obj


def fetch_24h_ticker_near() -> dict[str, Any] | None:
    try:
        r = httpx.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": TARGET_SPOT_PAIR},
            timeout=15.0,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def build_market_snapshot_en(ticker: dict[str, Any] | None) -> str:
    if not ticker:
        return (
            "Snapshot unavailable right now. Do not quote exact prices. "
            "Write using neutral structural language (consolidation, range, levels to watch) only."
        )
    last = _fmt_price_usdt(ticker.get("lastPrice"))
    pct = _fmt_percent(ticker.get("priceChangePercent"))
    high = _fmt_price_usdt(ticker.get("highPrice"))
    low = _fmt_price_usdt(ticker.get("lowPrice"))
    qv = _fmt_quote_volume(ticker.get("quoteVolume"))
    return (
        f"Binance spot pair {TARGET_PAIR_DISPLAY} (symbol {TARGET_SPOT_PAIR}), 24h:\n"
        f"- Last price: ~{last} USDT\n"
        f"- 24h change: {pct}%\n"
        f"- 24h high / low: ~{high} / ~{low} USDT\n"
        f"- 24h quote volume (USDT): ~{qv}\n"
        "Use only these rounded figures; write them in the post the same short way (no extra zeros)."
    )


def _chat_messages(*, market_snapshot: str, style_brief: str) -> list[dict[str, str]]:
    user = (
        f"{style_brief}\n\n"
        f"DATA SNAPSHOT:\n{market_snapshot}\n\n"
        "Write the Binance Feed post now (English only). Obey the STYLE line and all system rules."
    )
    return [
        {"role": "system", "content": SYSTEM_NEAR_FEED_PROMPT},
        {"role": "user", "content": user},
    ]


def generate_post_groq(*, market_snapshot: str) -> str:
    key = os.environ.get("GROQ_API_KEY", "").strip()
    if not key:
        print("Set GROQ_API_KEY", file=sys.stderr)
        sys.exit(1)

    from groq import Groq

    model = os.environ.get("GROQ_MODEL", "").strip() or DEFAULT_GROQ_MODEL
    style_brief = random.choice(STYLE_BRIEFS)
    temp = random.uniform(0.58, 0.84)
    client = Groq(api_key=key, timeout=120.0)
    resp = client.chat.completions.create(
        model=model,
        messages=_chat_messages(market_snapshot=market_snapshot, style_brief=style_brief),
        temperature=temp,
        max_tokens=650,
    )
    raw = (resp.choices[0].message.content or "").strip()
    text = _strip_wrapping(raw)
    if not text:
        sys.exit(1)
    return text


def _strip_wrapping(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def publish_square(body: str, api_key: str, *, content_extra: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {
        "X-Square-OpenAPI-Key": api_key,
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
    }
    extra = {k: v for k, v in (content_extra or {}).items() if k != "bodyTextOnly"}
    payload: dict[str, Any] = {**extra, "bodyTextOnly": body}
    r = httpx.post(SQUARE_ADD_URL, headers=headers, json=payload, timeout=30.0)
    r.raise_for_status()
    return r.json()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    ticker = fetch_24h_ticker_near()
    snapshot = build_market_snapshot_en(ticker)
    body = finalize_post_body(generate_post_groq(market_snapshot=snapshot))

    print("--- Post (EN) ---")
    print(body)
    print("-------------------")

    if args.dry_run:
        return

    bkey = os.environ.get("BINANCE_SQUARE_API_KEY")
    if not bkey:
        print("Set BINANCE_SQUARE_API_KEY", file=sys.stderr)
        sys.exit(1)

    try:
        data = publish_square(body, bkey, content_extra=load_square_content_extra())
    except httpx.HTTPError as e:
        send_telegram(f"Square HTTP error\n{TARGET_SPOT_PAIR}\n{e}")
        print(str(e), file=sys.stderr)
        sys.exit(1)

    code = data.get("code")
    if code != "000000":
        err = json.dumps(data, ensure_ascii=False, indent=2)
        print(err, file=sys.stderr)
        send_telegram(f"Square rejected\n{TARGET_SPOT_PAIR}\n{data.get('message') or err[:500]}")
        sys.exit(1)

    cid = (data.get("data") or {}).get("id")
    url = f"https://www.binance.com/square/post/{cid}" if cid else None
    if cid:
        print(f"Published: {url}")
    else:
        print("OK but no id")

    send_telegram(
        f"Published {TARGET_PAIR_DISPLAY}\n"
        + (f"{url}\n" if url else "")
        + f"\n{body[:600]}"
        + ("…" if len(body) > 600 else "")
    )


if __name__ == "__main__":
    main()
