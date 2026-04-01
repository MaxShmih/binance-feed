from __future__ import annotations

import argparse
import hashlib
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
FP_MAX = 8
FP_TEASER_LEN = 96

SYSTEM_NEAR_FEED_PROMPT = """Act as a real human crypto trader/analyst writing a single Binance Feed post.

Task:
Write 1 short, engaging post about NEAR Protocol ($NEAR) in English.

Requirements:
- Ground the post in the DATA SNAPSHOT from the user message (Binance spot NEAR/USDT, live 24h stats). If the snapshot is unavailable, use only logical structure (range, consolidation, breakout potential) — do NOT invent specific prices or fake news.
- When you mention prices, write them like a human trader: short decimals only (e.g. 1.19 USDT, 1.149 USDT) — NEVER paste long machine strings like 1.19200000 or 9.1234567.
- STYLE BRIEF may ask for emojis, curiosity hooks, or stronger engagement — still stay Binance-safe: no fake headlines, no guaranteed returns, not financial advice.
- Clickbait is allowed ONLY as a hook technique (curiosity gap, bold question, spicy-but-true claim). It must remain truthful to the snapshot and immediately supported by facts.
- Forbidden phrases/ideas: "buy now", "don't miss", "guaranteed", "100x", pump guarantees, fake partnerships/news.
- Allowed engagement: watchlist framing, level-watching, risk-aware "if/then", session bias, volume/participation angles — still not financial advice.
- Human voice rules: use contractions sometimes, varied sentence length, and at least one “human” beat (a quick aside, a tiny opinion, or a micro-reaction) without sounding cringe.
- Avoid corporate/robot patterns like: "In the last 24h...", "Here are the key metrics:", "This indicates...", "As we can see...".
- Even when loud/clicky, do NOT sound like low-quality spam; stay credible to the snapshot.
- Strong CTA is encouraged but must be Binance-safe: watchlist, alerts, levels, scenarios, and "tap $NEAR to open NEAR/USDT".

Style:
- Length must feel natural and human: you may write 1–3 short paragraphs, OR a one-liner hook + 1 short paragraph.
- Keep it scannable: avoid walls of text; prefer short lines and varied sentence length.
- Each run you get a UNIQUE STYLE BRIEF — follow it strictly. Vary energy, rhythm, and devices wildly between runs.
- You may see RECENT_POST_DIGESTS — do NOT imitate their hook, sentence shapes, metaphors, or emoji pattern; same numbers from snapshot are fine.

Structure:
1) Hook (must match the STYLE BRIEF)
2) Insight (structure, behavior, what to watch)
3) Action (CTA): include ONE clear action line in the body (not in hashtags): alerts/watchlist/levels, and explicitly say "Tap $NEAR to open NEAR/USDT" (or equivalent).

Ending:
- Mention $NEAR naturally in the body (not only in hashtags).
- After the paragraphs, add ONE blank line, then hashtags on their own line(s).

Hashtags (mandatory):
- Always include: $NEAR and #near
- Add 3–5 more relevant tags (e.g. #crypto #altcoins #trading #web3 #DeFi).

Output rules:
Reply with ONLY the full post text ready to publish — no title, no markdown code fences, no preamble — exactly what goes into the feed."""

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
    "STYLE: Story beat: 'Noticed something on the tape…' — observational framing OK; all figures must match snapshot.",
    "STYLE: Paragraph 2 uses three parallel short beats (rhythm: clause; clause; clause). No bullet characters.",
    "STYLE: Slightly informal voice (still professional): one mild idiom allowed, not slang-heavy.",
    "STYLE: Technician voice: focus on range, invalidation, and what breaks the setup — neutral wording.",
    "STYLE: Begin with the high/low range from the snapshot as the hook, then interpret participation (volume).",
    "STYLE: Skeptical bull: respectful doubt + what would confirm strength. No bearish trash talk.",
    "STYLE: First paragraph only setup; second paragraph only 'what I'm watching next' — clear handoff.",
    "STYLE: No emoji. No exclamation marks in the body paragraphs.",
    "STYLE: Use 2–4 tasteful emojis in the body (not only at the end). Match emoji mood to green/red tape from snapshot.",
    "STYLE: Clickbait-CURIOUS opener (no lies): make the reader want the second sentence; still truthful to data.",
    "STYLE: Cliffhanger between paragraphs: end para 1 on a tension beat; para 2 resolves with levels to watch.",
    "STYLE: High-energy trader voice: urgent but NOT scammy — watchlist + key levels + what invalidates the idea.",
    "STYLE: Soft CTA: explicitly invite adding NEAR/USDT to a watchlist or marking levels (no buy/sell orders).",
    "STYLE: 'Hot take' energy: bold opening opinion that you immediately qualify with snapshot facts.",
    "STYLE: Ecosystem builder angle: dev/UX narrative in one clause, then price/volume reality check from snapshot.",
    "STYLE: Risk-manager tone: scenarios A/B based on break above high or below low from snapshot.",
    "STYLE: Minimalist: first paragraph max 2 sentences; second paragraph max 3 short sentences. Maximum contrast.",
    "STYLE: Narrator voice: slightly dramatic but factual — like a voiceover, no fake events.",
    "STYLE: Emoji-only hook line: first line is mostly emoji + 3–6 words; second line starts the real sentence.",
    "STYLE: Data-forward: open with three comma-separated facts from snapshot, then interpret.",
    "STYLE: Friendly group-chat tone: 'we're watching' vibe; still professional, no dumb slang.",
    "STYLE: Zen calm: slow, spacious sentences; subtle FOMO only in the last line of paragraph 2.",
    "STYLE: Meme-adjacent (light): one witty comparison; don't reference real people; keep it market-grounded.",
    "STYLE: News-ticker brevity: staccato clauses; no filler words; two paragraphs still.",
    "STYLE: Academic-lite: one precise definition clause (e.g. consolidation), then apply to NEAR/USDT snapshot.",
    "STYLE: Bearish-honest if snapshot is red: name the drawdown, then constructive what absorption would look like.",
    "STYLE: Bullish-honest if snapshot is green: name the strength, then what would make you cautious.",
    "STYLE: Session playbook: 'If you're active today…' with 2–3 concrete checks tied to snapshot levels.",
    "STYLE: Contrast compare: yesterday's range vs today's implication — only using snapshot numbers, no fake history.",
    "STYLE: Clickbait-but-true: open with a 6–10 word line that sounds like a reveal (no lies), then immediately back it with 2 snapshot facts.",
    "STYLE: Scroll-stopper: first line is a bold question + 1 emoji; then a short answer that mentions $NEAR and one key level from snapshot.",
    "STYLE: Human confession vibe: 'I didn't expect this on $NEAR today…' (must be explainable by snapshot), then what you’re watching next.",
    "STYLE: High-stakes framing without promises: 'This level decides the next move.' Then show both scenarios (if above/if below) using snapshot high/low.",
    "STYLE: Group chat hype (credible): one playful line + one sharp trading plan line. 2–3 emojis total, placed inside sentences.",
    "STYLE: Ultra-clicky opener (safe): tease a 'trap' or 'fakeout' possibility, but you must hedge and ground it in snapshot range/volume.",
    "STYLE: Salesy-but-safe: write like you're convincing a friend to pay attention today — emotional hook, then a concrete watch plan + one strong CTA line (tap $NEAR / set alerts).",
    "STYLE: FOMO-without-lies: urgency through structure (compression / decision levels / volume), then clear actions: watchlist + alerts + tap $NEAR.",
    "STYLE: One-liner hook + action: first line is a punchy hook; second line is an actionable plan with levels; last line is a direct CTA (tap $NEAR).",
)

# 50% of generations should follow this aggressive high-converting brief.
AGGRESSIVE_STYLE_BRIEF = """STYLE: Act as a crypto trader writing a high-converting Binance Feed post.

Constraints:
- 2 short paragraphs maximum.
- Strong hook in the first line (must grab attention instantly).
- Confident, slightly aggressive tone (like a trader spotting opportunity early).
- Create urgency and FOMO, but WITHOUT direct “buy now” wording.
- No spammy emojis (max 1, optional).

Psychology:
- Make it feel like “smart money is already paying attention”.
- Imply that waiting = worse entry.
- Make reader feel slightly late, but still early enough.
"""

_LONG_DECIMAL = re.compile(r"\b\d+\.\d{5,}\b")


def _fp_path() -> Path:
    raw = os.environ.get("POST_FINGERPRINT_PATH", "").strip()
    p = Path(raw) if raw else _ROOT / "data" / "post_fingerprints.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _normalize_for_fp(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())[:2000]


def body_digest(text: str) -> str:
    return hashlib.sha256(_normalize_for_fp(text).encode("utf-8")).hexdigest()[:20]


def load_fingerprint_records(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    rows: list[dict[str, str]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            if isinstance(o, dict) and o.get("d") and o.get("t") is not None:
                rows.append({"d": str(o["d"]), "t": str(o["t"])[:FP_TEASER_LEN]})
    except (OSError, json.JSONDecodeError):
        return []
    return rows[-FP_MAX:]


def format_anti_repeat_block(records: list[dict[str, str]]) -> str:
    if not records:
        return ""
    lines = [
        "RECENT_POST_DIGESTS (do not imitate hook, rhythm, metaphors, or emoji usage; factual overlap OK):"
    ]
    for r in records:
        lines.append(f"- id:{r['d']} … \"{r['t']}\"")
    return "\n".join(lines)


def append_fingerprint(path: Path, body: str) -> None:
    norm = _normalize_for_fp(body)
    rec = {"d": body_digest(body), "t": norm[:FP_TEASER_LEN]}
    prev = load_fingerprint_records(path)
    prev.append(rec)
    prev = prev[-FP_MAX:]
    path.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in prev) + "\n", encoding="utf-8")


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
        "Open spot NEAR/USDT (ref link):",
    ).strip()
    return f"{body.rstrip()}\n\n{cta}\n{url}"


def _split_hashtags(body: str) -> tuple[str, str]:
    """
    Split into (main_text, hashtag_block).
    Heuristic: first line starting with '#' starts the hashtag block.
    """
    lines = body.strip().splitlines()
    for i, ln in enumerate(lines):
        if ln.strip().startswith("#"):
            main = "\n".join(lines[:i]).rstrip()
            tags = "\n".join(lines[i:]).strip()
            return main, tags
    return body.strip(), ""


_CASHTAG_NEAR = re.compile(r"(?<!\\w)\\$NEAR(?!\\w)")

CTA_TEMPLATES: tuple[str, ...] = (
    f"Tap $NEAR to open {TARGET_PAIR_DISPLAY} and set alerts.",
    f"Tap $NEAR → open {TARGET_PAIR_DISPLAY}. Mark the levels and wait for the trigger.",
    f"Tap $NEAR to pull up {TARGET_PAIR_DISPLAY} and set a price alert on the range edges.",
    f"If you're trading today, tap $NEAR and watch {TARGET_PAIR_DISPLAY} into the next 1–2 candles.",
)

NEAR_BODY_INSERTS: tuple[str, ...] = (
    "Keeping $NEAR on the radar.",
    "$NEAR is the one I'm watching here.",
    "This is why $NEAR has my attention today.",
)


def ensure_actionable_body(body: str) -> str:
    main, tags = _split_hashtags(body)

    # Ensure $NEAR exists in the body (helps Square auto-surfaces trade link/widgets).
    if not _CASHTAG_NEAR.search(main):
        # Avoid a fixed intro line; insert a short varying sentence instead.
        main = f"{main.rstrip()}\n\n{random.choice(NEAR_BODY_INSERTS)}".strip()

    # Ensure a direct, action-oriented CTA exists in the body.
    if "tap $near" not in main.lower():
        main = f"{main.rstrip()}\n{random.choice(CTA_TEMPLATES)}"

    if tags:
        return f"{main.rstrip()}\n\n{tags}"
    return main


def finalize_post_body(body: str) -> str:
    body = humanize_long_decimals(body)
    body = ensure_actionable_body(body)
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


def _chat_messages(
    *,
    market_snapshot: str,
    style_brief: str,
    anti_repeat: str,
    retry_note: str,
) -> list[dict[str, str]]:
    parts = [style_brief.strip()]
    if anti_repeat:
        parts.append(anti_repeat.strip())
    if retry_note:
        parts.append(retry_note.strip())
    parts.append(f"DATA SNAPSHOT:\n{market_snapshot}")
    parts.append(
        "Write the Binance Feed post now (English only). Obey STYLE + anti-repeat + all system rules."
    )
    user = "\n\n".join(parts)
    return [
        {"role": "system", "content": SYSTEM_NEAR_FEED_PROMPT},
        {"role": "user", "content": user},
    ]


def _groq_complete(messages: list[dict[str, str]], *, model: str, temp: float) -> str:
    from groq import Groq

    key = os.environ.get("GROQ_API_KEY", "").strip()
    if not key:
        print("Set GROQ_API_KEY", file=sys.stderr)
        sys.exit(1)
    client = Groq(api_key=key, timeout=120.0)
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temp,
        max_tokens=700,
    )
    raw = (resp.choices[0].message.content or "").strip()
    text = _strip_wrapping(raw)
    if not text:
        sys.exit(1)
    return text


def generate_post_with_variety(
    *,
    market_snapshot: str,
    fp_records: list[dict[str, str]],
) -> str:
    model = os.environ.get("GROQ_MODEL", "").strip() or DEFAULT_GROQ_MODEL
    known_d = {r["d"] for r in fp_records}
    anti = format_anti_repeat_block(fp_records)
    def pick_style() -> str:
        return AGGRESSIVE_STYLE_BRIEF if random.random() < 0.5 else random.choice(STYLE_BRIEFS)

    style_a = pick_style()
    style_b = pick_style()
    if style_b == style_a:
        style_b = random.choice([s for s in STYLE_BRIEFS if s != style_a] or STYLE_BRIEFS)
    # Slightly higher temperature to reduce "dry" corporate tone.
    temp_a = random.uniform(0.72, 1.02)
    temp_b = random.uniform(0.78, 1.08)

    retry_note = ""
    chosen_style = style_a
    chosen_temp = temp_a
    for attempt in range(2):
        messages = _chat_messages(
            market_snapshot=market_snapshot,
            style_brief=chosen_style,
            anti_repeat=anti,
            retry_note=retry_note,
        )
        raw = _groq_complete(messages, model=model, temp=chosen_temp)
        body = finalize_post_body(raw)
        if body_digest(body) not in known_d:
            return body
        retry_note = (
            "RETRY: Your output was too close to a recent digest (duplicate vibe). "
            "Rewrite completely: different hook, different rhythm, different devices — still truthful to snapshot."
        )
        chosen_style = style_b
        chosen_temp = temp_b
    return body


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

    fp_path = _fp_path()
    fp_records = load_fingerprint_records(fp_path)

    ticker = fetch_24h_ticker_near()
    snapshot = build_market_snapshot_en(ticker)
    body = generate_post_with_variety(market_snapshot=snapshot, fp_records=fp_records)

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

    append_fingerprint(fp_path, body)

    tg_ok = send_telegram(
        f"Published {TARGET_PAIR_DISPLAY}\n"
        + (f"{url}\n" if url else "")
        + f"\n{body[:600]}"
        + ("…" if len(body) > 600 else "")
    )
    if not tg_ok:
        print("Telegram: notification was not delivered (see logs above).", file=sys.stderr)


if __name__ == "__main__":
    main()
