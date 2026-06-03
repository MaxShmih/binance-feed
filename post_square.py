from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import random
import re
import sys
import time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, TypedDict

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

SYSTEM_NEAR_FEED_PROMPT = """
You are a top 0.1% Binance Square creator known for generating highly engaging crypto posts that consistently attract views, likes, comments, shares, and follows.

Your task is to write ONE Binance Square post in English about $NEAR for spot traders.

PRIMARY GOAL:
Make people stop scrolling and read the entire post.

SECONDARY GOAL:
Increase engagement through curiosity, emotion, trader psychology, and clear market observations.

DATA RULES:

* Use ONLY information contained in SNAPSHOT.
* NEVER invent prices, percentages, volume, support, resistance, market events, catalysts, news, or technical levels.
* NEVER assume future price movement as fact.
* NEVER fabricate bullish or bearish signals.
* Every market observation must be supported by SNAPSHOT data.

CONTENT REQUIREMENTS:

* Mention $NEAR naturally in the body.
* Use at least 2 snapshot metrics naturally inside the narrative.
* Explain what the current position inside the 24h range means.
* Explain why the 24h change matters.
* Explain what traders should monitor next.
* Convert raw numbers into a story, not a report.

PSYCHOLOGY:
The post should feel like an experienced trader sharing an insight others may have missed.

Create:

* curiosity
* anticipation
* discussion
* trader FOMO (without hype)
* fear of missing information, NOT fear of missing profits

GOOD THEMES:

* momentum building
* compression
* range positioning
* crowd psychology
* accumulation behavior
* market hesitation
* breakout watch
* trend continuation watch
* hidden strength
* hidden weakness
* unusual volume behavior

AVOID:

* "interesting times"
* "stay tuned"
* "let's see"
* "volatile market"
* "market participants"
* "always do your own research"
* "this could be huge"
* "100x"
* "moon"
* "lambo"
* generic AI phrases
* corporate language
* dry reporting

WRITING STYLE:

* Human.
* Confident.
* Specific.
* Fast-paced.
* Social-media native.
* Short paragraphs.
* Strong rhythm.
* No fluff.

HOOK REQUIREMENTS:
The first line is critical.

Use one of these styles:

* Contrarian observation
* Unexpected statistic
* Trader psychology insight
* Hidden opportunity
* Sharp question
* Bold but data-backed statement

Examples of hook style:

* "Most traders are watching the wrong level on $NEAR."
* "$NEAR is telling a different story than the headline number suggests."
* "This is where traders usually start paying attention."
* "The current setup on $NEAR is more interesting than it looks."
* "One number in today's $NEAR data stands out."

POST STRUCTURE:

1. Hook
2. Context from snapshot
3. Why it matters
4. What traders should watch next
5. Engagement-driving closing line

ENDING:
Finish with a short question that encourages comments.

Examples:

* "What are you watching on $NEAR right now?"
* "Do you see strength here or more range-bound action?"
* "Would you be paying attention to this setup?"

Then add a blank line.

Then add:
#near

Plus 2–4 highly relevant crypto hashtags.

DIVERSITY RULE:
If RECENT posts are provided:

* NEVER reuse the same opening.
* NEVER reuse the same narrative angle.
* NEVER reuse the same structure.
* Generate a completely fresh perspective even when using the same snapshot.

OUTPUT:
Return ONLY the finished post text.
Do not explain.
Do not use markdown fences.
"""

# One FORMAT brief per run — keeps shape different (this drives variety more than a long system prompt).
FORMAT_BRIEFS: tuple[str, ...] = (
    "FORMAT: Exactly 3 short lines before hashtags. Line1 = hook. Line2 = one snapshot fact. Line3 = what you'd watch next. Then blank line, then hashtags.",
    "FORMAT: One compact paragraph (2–3 sentences), then blank line, then hashtags.",
    "FORMAT: Line1 = a question to the reader. Line2–3 = answer with snapshot data. Then blank line, then hashtags.",
    "FORMAT: Telegram vibe: 2–4 very short lines, no 'essay' feel. Then blank line, then hashtags.",
    "FORMAT: Start with 'Quick read:' then one more line only. Then blank line, then hashtags.",
    "FORMAT: Two lines that start with '- ' (mini bullets), then one normal sentence if you need it. Then blank line, then hashtags.",
    "FORMAT: Use '1)' and '2)' exactly two lines only (two beats). Then blank line, then hashtags.",
    "FORMAT: First line = one short ALL-CAPS hook phrase. Next 1–2 lines normal case. Then blank line, then hashtags.",
    "FORMAT: Mostly lowercase except $NEAR and USDT and numbers. 2–4 lines total before hashtags.",
    "FORMAT: No emojis at all. Dry/humorless trader note. Then hashtags.",
    "FORMAT: At most one emoji in the whole body, mid-line. Then hashtags.",
    "FORMAT: Start with 'Bias:' one word + comma + one reason from snapshot. Then 1–2 lines detail. Then hashtags.",
    "FORMAT: Contrast: 'Bulls see … / Bears see …' in two short lines (no fake news). Then hashtags.",
    "FORMAT: Micro-story: 'Noticed … on the tape' → one twist → one watch item (all from snapshot). Then hashtags.",
)

AGGRESSIVE_FORMAT_BRIEF = (
    "FORMAT: High-converting trader energy: max 2 short paragraphs before hashtags. "
    "Line1 must hit hard. Urgency from structure (range, breakout risk, vol) — no buy-now language, no guarantees. "
    "Smart-money vibe OK. Optional: at most 1 emoji. Then blank line, then hashtags."
)

QUALITY_FORMAT_BRIEFS: tuple[str, ...] = (
    "FORMAT: Open with a one-line thesis (bullish/bearish/neutral) tied to 24h % and range position. "
    "Then 2–3 sentences: price, hi/lo context, qVol read. One line what you'd watch next. Blank line, hashtags.",
    "FORMAT: 'Tape read:' then 3 tight lines — (1) last vs range (2) vol/momentum (3) invalidation or next trigger. Blank line, hashtags.",
    "FORMAT: Name the 24h high and low as levels; say where last sits between them (% vibe OK if in SNAPSHOT). "
    "One sentence on whether vol supports the move. Blank line, hashtags.",
    "FORMAT: Question hook, then answer in 2–3 sentences using only snapshot data. End with one concrete watch item. Blank line, hashtags.",
)

_LONG_DECIMAL = re.compile(r"\b\d+\.\d{5,}\b")


def _fp_path() -> Path:
    raw = os.environ.get("POST_FINGERPRINT_PATH", "").strip()
    p = Path(raw) if raw else _ROOT / "data" / "post_fingerprints.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _fp_path_for_account(account: str) -> Path:
    """
    Per-account fingerprint history prevents cross-account repeats and avoids races
    when multiple accounts run in parallel.
    """
    raw = os.environ.get("POST_FINGERPRINT_PATH", "").strip()
    if raw:
        p = Path(raw)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    raw_dir = os.environ.get("POST_FINGERPRINT_DIR", "").strip()
    base = Path(raw_dir) if raw_dir else (_ROOT / "data")
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", (account or "default").strip())[:64] or "default"
    p = base / f"post_fingerprints.{safe}.jsonl"
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
    raw_cap = os.environ.get("GROQ_ANTI_REPEAT_TEASERS", "4").strip()
    try:
        cap = int(raw_cap)
    except ValueError:
        cap = 4
    cap = max(0, min(cap, FP_MAX))
    if cap == 0 or not records:
        return ""
    use = records[-cap:]
    lines = ["RECENT (vary hook/shape; nums OK):"]
    for r in use:
        lines.append(f"- {r['d']}: \"{r['t']}\"")
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
    f"Tap $NEAR → open {TARGET_PAIR_DISPLAY}; mark the range edges.",
    f"If you're active: tap $NEAR, pull up {TARGET_PAIR_DISPLAY}, set alerts.",
)

SOFT_CTA_TEMPLATES: tuple[str, ...] = (
    f"Worth keeping {TARGET_PAIR_DISPLAY} on the watchlist today.",
    f"I'm marking levels on {TARGET_PAIR_DISPLAY} and waiting for a clean trigger.",
    f"Price alerts on {TARGET_PAIR_DISPLAY} beat guessing the tape.",
)

NEAR_BODY_INSERTS: tuple[str, ...] = (
    "$NEAR — on my screen today.",
    "Watching $NEAR vs this range.",
    "Current read: $NEAR, spot tape.",
)


def ensure_actionable_body(body: str) -> str:
    main, tags = _split_hashtags(body)

    # Light touch only: keep Square cashtag surfaces without forcing the same intro every time.
    if not _CASHTAG_NEAR.search(main):
        main = f"{main.rstrip()}\n{random.choice(NEAR_BODY_INSERTS)}".strip()

    # Optional CTA — always pushing "tap $NEAR" made posts feel identical.
    lower = main.lower()
    if "tap $near" not in lower and random.random() < float(
        os.environ.get("SQUARE_TAP_CTA_PROB", "0.25")
    ):
        main = f"{main.rstrip()}\n{random.choice(CTA_TEMPLATES)}"
    elif "watchlist" not in lower and "alert" not in lower and random.random() < 0.35:
        main = f"{main.rstrip()}\n{random.choice(SOFT_CTA_TEMPLATES)}"

    out = f"{main.rstrip()}\n\n{tags}".strip() if tags else main.rstrip()
    return re.sub(r"\n{3,}", "\n\n", out).strip()


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
    range_note = ""
    last_d = _to_decimal(ticker.get("lastPrice"))
    high_d = _to_decimal(ticker.get("highPrice"))
    low_d = _to_decimal(ticker.get("lowPrice"))
    if last_d is not None and high_d is not None and low_d is not None and high_d > low_d:
        pos = ((last_d - low_d) / (high_d - low_d) * Decimal("100")).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
        range_note = f" Last is ~{pos}% up from 24h low toward high (within hi/lo band)."
    return (
        f"{TARGET_PAIR_DISPLAY} 24h: last ~{last} USDT, {pct}%, hi/lo ~{high}/{low}, qVol ~{qv} USDT.{range_note} "
        "Use only these figures; short decimals in post."
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
    parts.append(f"SNAPSHOT:\n{market_snapshot}")
    parts.append(
        "Write the post (EN). Strong hook, concrete snapshot numbers, one clear takeaway. FORMAT + rules above."
    )
    user = "\n\n".join(parts)
    return [
        {"role": "system", "content": SYSTEM_NEAR_FEED_PROMPT},
        {"role": "user", "content": user},
    ]


def _is_llm_rate_limit(exc: BaseException) -> bool:
    if type(exc).__name__ == "RateLimitError":
        return True
    if getattr(exc, "status_code", None) == 429:
        return True
    err = f"{type(exc).__name__}: {exc!s}"
    low = err.lower()
    return "429" in err or "rate_limit" in low or "rate limit" in low or "tpd" in low or "tpm" in low


def _normalize_openai_base_url(raw: str) -> str:
    u = raw.strip().rstrip("/")
    if not u.endswith("/v1"):
        u = f"{u}/v1"
    return u


def _llm_proxy_config() -> tuple[str, str] | None:
    """FreeLLMAPI / any OpenAI-compatible proxy (FREELLMAPI_* or LLM_* env)."""
    base = (os.environ.get("FREELLMAPI_BASE_URL") or os.environ.get("LLM_BASE_URL") or "").strip()
    key = (os.environ.get("FREELLMAPI_API_KEY") or os.environ.get("LLM_API_KEY") or "").strip()
    if base and key:
        return _normalize_openai_base_url(base), key
    return None


def _llm_model() -> str:
    if _llm_proxy_config():
        return (
            os.environ.get("FREELLMAPI_MODEL", "").strip()
            or os.environ.get("GROQ_MODEL", "").strip()
            or DEFAULT_GROQ_MODEL
        )
    return os.environ.get("GROQ_MODEL", "").strip() or DEFAULT_GROQ_MODEL


def _llm_models_to_try(primary: str) -> list[str]:
    """Primary model, then optional fallback when using FreeLLMAPI."""
    models = [primary]
    if not _llm_proxy_config():
        return models
    fb = (
        os.environ.get("FREELLMAPI_MODEL_FALLBACK", "").strip()
        or DEFAULT_GROQ_MODEL
    )
    if fb and fb not in models:
        models.append(fb)
    return models


def _proxy_empty_retries() -> int:
    raw = os.environ.get("FREELLMAPI_EMPTY_RETRIES", "3").strip()
    try:
        n = int(raw)
    except ValueError:
        return 3
    return max(1, min(n, 5))


def _groq_max_tokens() -> int:
    raw = os.environ.get("GROQ_MAX_TOKENS", "450").strip()
    try:
        n = int(raw)
    except ValueError:
        return 450
    return max(120, min(n, 700))


def _groq_generation_attempts() -> int:
    raw = os.environ.get("GROQ_GENERATION_ATTEMPTS", "2").strip()
    try:
        n = int(raw)
    except ValueError:
        return 2
    return max(1, min(n, 3))


def _llm_complete(
    messages: list[dict[str, str]],
    *,
    model: str,
    temp: float,
    api_key: str,
) -> str:
    proxy = _llm_proxy_config()
    max_tok = _groq_max_tokens()
    empty_retries = _proxy_empty_retries() if proxy else 1

    if proxy:
        from openai import OpenAI

        base_url, proxy_key = proxy
        client = OpenAI(base_url=base_url, api_key=proxy_key, timeout=120.0)
        provider_label = "FreeLLMAPI"
    else:
        from groq import Groq

        key = (api_key or "").strip()
        if not key:
            raise RuntimeError("Set GROQ_API_KEY (or per-account GROQ_API_KEY_<NAME>), or FREELLMAPI_BASE_URL + FREELLMAPI_API_KEY")
        client = Groq(api_key=key, timeout=120.0)
        provider_label = "Groq"

    last_finish: str | None = None
    last_model: str | None = None
    for try_model in _llm_models_to_try(model):
        for attempt in range(empty_retries):
            try:
                resp = client.chat.completions.create(
                    model=try_model,
                    messages=messages,
                    temperature=temp,
                    max_tokens=max_tok,
                )
            except Exception as e:
                if _is_llm_rate_limit(e):
                    raise RuntimeError(
                        f"{provider_label}: rate limit — {e!s}. "
                        "Proxy will retry other keys on next run; or lower cron frequency / GROQ_MAX_TOKENS."
                    ) from e
                raise

            choice = resp.choices[0]
            last_finish = getattr(choice, "finish_reason", None)
            last_model = getattr(resp, "model", None) or try_model
            raw = (choice.message.content or "").strip()
            text = _strip_wrapping(raw)
            if text:
                if attempt > 0 or try_model != model:
                    print(
                        f"{provider_label}: ok model={last_model!r} "
                        f"(after retry {attempt + 1}, requested={model!r})",
                        file=sys.stderr,
                    )
                return text

            print(
                f"{provider_label}: empty response attempt {attempt + 1}/{empty_retries} "
                f"model={last_model!r} finish_reason={last_finish!r}",
                file=sys.stderr,
            )
            if attempt + 1 < empty_retries:
                time.sleep(1.2 * (attempt + 1))

    raise RuntimeError(
        f"{provider_label}: empty model response "
        f"(last model={last_model!r}, finish_reason={last_finish!r})"
    )


def generate_post_with_variety(
    *,
    market_snapshot: str,
    fp_records: list[dict[str, str]],
    groq_api_key: str,
) -> str:
    model = _llm_model()
    known_d = {r["d"] for r in fp_records}
    anti = format_anti_repeat_block(fp_records)
    def pick_format() -> str:
        r = random.random()
        if r < 0.30:
            return AGGRESSIVE_FORMAT_BRIEF
        if r < 0.70:
            return random.choice(QUALITY_FORMAT_BRIEFS)
        return random.choice(FORMAT_BRIEFS)

    all_formats = (*FORMAT_BRIEFS, *QUALITY_FORMAT_BRIEFS, AGGRESSIVE_FORMAT_BRIEF)
    style_a = pick_format()
    style_b = pick_format()
    if style_b == style_a:
        pool = [s for s in all_formats if s != style_a] or list(all_formats)
        style_b = random.choice(pool)
    temp_a = random.uniform(0.78, 0.96)
    temp_b = random.uniform(0.82, 1.0)

    retry_note = ""
    chosen_style = style_a
    chosen_temp = temp_a
    attempts = _groq_generation_attempts()
    for attempt in range(attempts):
        messages = _chat_messages(
            market_snapshot=market_snapshot,
            style_brief=chosen_style,
            anti_repeat=anti,
            retry_note=retry_note,
        )
        raw = _llm_complete(messages, model=model, temp=chosen_temp, api_key=groq_api_key)
        body = finalize_post_body(raw)
        if body_digest(body) not in known_d:
            return body
        retry_note = "RETRY: new hook + shape vs digests; same snapshot numbers OK."
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


class _AccountCfg(TypedDict):
    name: str
    square_api_key: str
    groq_api_key: str


def _parse_key_list(raw: str) -> list[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
        except json.JSONDecodeError:
            return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _get_key_by_name(prefix: str, name: str) -> str:
    suffix = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    candidates = [
        f"{prefix}_{suffix}",
        f"{prefix}_{suffix.upper()}",
        f"{prefix}_{suffix.lower()}",
    ]
    for env_name in candidates:
        v = os.environ.get(env_name, "").strip()
        if v:
            return v
    return ""


def _parse_account_configs_from_env() -> list[_AccountCfg]:
    """
    Supported:
    - BINANCE_SQUARE_ACCOUNTS=acc1,acc2,acc3 and BINANCE_SQUARE_API_KEY_ACC1=... (case-insensitive by suffix)
      + GROQ_API_KEY_ACC1=... (same names), unless FREELLMAPI_BASE_URL + FREELLMAPI_API_KEY are set
    - BINANCE_SQUARE_API_KEYS=key1,key2,key3  (or JSON array)
      + GROQ_API_KEYS=key1,key2,key3          (same length)
    - BINANCE_SQUARE_API_KEY=single
      + GROQ_API_KEY=single
    """
    use_proxy = _llm_proxy_config() is not None
    accs_raw = os.environ.get("BINANCE_SQUARE_ACCOUNTS", "").strip()
    if accs_raw:
        names = [x.strip() for x in accs_raw.split(",") if x.strip()]
        out: list[_AccountCfg] = []
        missing_sq: list[str] = []
        missing_groq: list[str] = []
        for name in names:
            sq = _get_key_by_name("BINANCE_SQUARE_API_KEY", name)
            if not sq:
                missing_sq.append(name)
                continue
            g = ""
            if not use_proxy:
                g = _get_key_by_name("GROQ_API_KEY", name)
                if not g:
                    missing_groq.append(name)
                    g = os.environ.get("GROQ_API_KEY", "").strip()
                if not g:
                    missing_groq.append(name)
            out.append({"name": name, "square_api_key": sq, "groq_api_key": g})
        if missing_sq or missing_groq:
            msg = ["BINANCE_SQUARE_ACCOUNTS mode: missing keys."]
            if missing_sq:
                msg.append("Missing BINANCE_SQUARE_API_KEY_<NAME> for: " + ", ".join(sorted(set(missing_sq))))
            if missing_groq:
                msg.append("Missing GROQ_API_KEY_<NAME> (or GROQ_API_KEY fallback) for: " + ", ".join(sorted(set(missing_groq))))
            print("\n".join(msg), file=sys.stderr)
            sys.exit(1)
        return out

    keys_raw = os.environ.get("BINANCE_SQUARE_API_KEYS", "").strip()
    if keys_raw:
        sq_keys = _parse_key_list(keys_raw)
        groq_keys = [] if use_proxy else _parse_key_list(os.environ.get("GROQ_API_KEYS", ""))
        if groq_keys and len(groq_keys) != len(sq_keys):
            print(
                f"GROQ_API_KEYS length mismatch: got {len(groq_keys)} but BINANCE_SQUARE_API_KEYS has {len(sq_keys)}",
                file=sys.stderr,
            )
            sys.exit(1)
        out: list[_AccountCfg] = []
        for i, sq in enumerate(sq_keys):
            name = f"acc{i+1}"
            g = ""
            if not use_proxy:
                g = groq_keys[i] if groq_keys else os.environ.get("GROQ_API_KEY", "").strip()
            out.append({"name": name, "square_api_key": sq, "groq_api_key": g})
        return out

    single = os.environ.get("BINANCE_SQUARE_API_KEY", "").strip()
    if not single:
        return []
    groq = "" if use_proxy else os.environ.get("GROQ_API_KEY", "").strip()
    return [{"name": "default", "square_api_key": single, "groq_api_key": groq}]


def _run_for_account(*, cfg: _AccountCfg, snapshot: str, dry_run: bool) -> str | None:
    account = cfg["name"]
    fp_path = _fp_path_for_account(account)
    fp_records = load_fingerprint_records(fp_path)
    body = generate_post_with_variety(
        market_snapshot=snapshot,
        fp_records=fp_records,
        groq_api_key=cfg.get("groq_api_key", ""),
    )

    print(f"--- Post (EN) [{account}] ---")
    print(body)
    print("-----------------------------")

    if dry_run:
        return None

    try:
        data = publish_square(body, cfg["square_api_key"], content_extra=load_square_content_extra())
    except httpx.HTTPError as e:
        send_telegram(f"Square HTTP error\n{TARGET_SPOT_PAIR}\naccount={account}\n{e}")
        raise

    code = data.get("code")
    if code != "000000":
        msg = data.get("message")
        msg_l = (str(msg) or "").lower()
        err = json.dumps(data, ensure_ascii=False, indent=2)
        if str(code) == "220009" or "limit" in msg_l or "exceed" in msg_l:
            print(
                f"[{account}] Hint: Square OpenAPI hit a frequency/daily limit. "
                "Reduce cron frequency per account or stagger schedules.",
                file=sys.stderr,
            )
        send_telegram(
            f"Square rejected\n{TARGET_SPOT_PAIR}\naccount={account}\n{data.get('message') or err[:500]}"
        )
        raise RuntimeError(f"Square API rejected for {account}: code={code!r} message={msg!r}")

    cid = (data.get("data") or {}).get("id")
    url = f"https://www.binance.com/square/post/{cid}" if cid else None
    if cid:
        print(f"[{account}] Published: {url}")
    else:
        print(f"[{account}] OK but no id")

    append_fingerprint(fp_path, body)

    tg_ok = send_telegram(
        f"Published {TARGET_PAIR_DISPLAY}\n"
        + f"account={account}\n"
        + (f"{url}\n" if url else "")
        + f"\n{body[:600]}"
        + ("…" if len(body) > 600 else "")
    )
    if not tg_ok:
        print(f"[{account}] Telegram: notification was not delivered (see logs above).", file=sys.stderr)
    return url


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--show-accounts",
        action="store_true",
        help="Print parsed account names (no secrets) and exit.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=0,
        help="Parallel accounts limit (0 = auto).",
    )
    args = p.parse_args()

    trig = os.environ.get("CRON_TRIGGER_ID", "").strip()
    if trig:
        print(f"trigger_id={trig}", file=sys.stderr)

    ticker = fetch_24h_ticker_near()
    snapshot = build_market_snapshot_en(ticker)

    accounts = _parse_account_configs_from_env()
    accounts = [a for a in accounts if a.get("square_api_key")]
    proxy = _llm_proxy_config()
    need_groq = not proxy and any(not a.get("groq_api_key") for a in accounts)
    if not accounts or need_groq:
        print(
            "Missing keys. Set:\n"
            "- FREELLMAPI_BASE_URL + FREELLMAPI_API_KEY (proxy; Groq keys optional)\n"
            "  OR\n"
            "- BINANCE_SQUARE_ACCOUNTS + BINANCE_SQUARE_API_KEY_<NAME> + GROQ_API_KEY_<NAME>\n"
            "  OR\n"
            "- BINANCE_SQUARE_API_KEYS + GROQ_API_KEYS (same length)\n"
            "  OR\n"
            "- BINANCE_SQUARE_API_KEY + GROQ_API_KEY",
            file=sys.stderr,
        )
        sys.exit(1)
    if proxy:
        print(f"LLM via proxy: {_normalize_openai_base_url(os.environ.get('FREELLMAPI_BASE_URL') or os.environ.get('LLM_BASE_URL', ''))} model={_llm_model()}", file=sys.stderr)

    if args.show_accounts:
        print("Parsed accounts:")
        for a in accounts:
            print(f"- {a['name']}")
        return

    if args.dry_run:
        for cfg in accounts:
            _run_for_account(cfg=cfg, snapshot=snapshot, dry_run=True)
        return

    if args.max_workers and args.max_workers > 0:
        max_workers = args.max_workers
    elif proxy and os.environ.get("FREELLMAPI_PARALLEL_ACCOUNTS", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        # One LLM request at a time — avoids empty responses on small FreeLLMAPI VPS.
        max_workers = 1
    else:
        max_workers = min(8, len(accounts))
    failures: list[str] = []
    published: list[tuple[str, str | None]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_run_for_account, cfg=cfg, snapshot=snapshot, dry_run=False): cfg["name"] for cfg in accounts}
        for fut in concurrent.futures.as_completed(futs):
            name = futs[fut]
            try:
                url = fut.result()
                published.append((name, url))
            except Exception as e:
                failures.append(f"{name}: {type(e).__name__}: {e}")

    # One compact summary ping per cron run (useful when running 3 accounts in parallel).
    summary_lines = [f"Square run summary ({TARGET_PAIR_DISPLAY})"]
    if published:
        for name, url in sorted(published):
            summary_lines.append(f"OK  account={name}" + (f"  {url}" if url else ""))
    if failures:
        for f in failures:
            summary_lines.append(f"ERR {f}")
    send_telegram("\n".join(summary_lines))

    if failures:
        # Exit non-zero so cron can alert; Telegram is already attempted per-account on known failures.
        print("Some accounts failed:\n- " + "\n- ".join(failures), file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
