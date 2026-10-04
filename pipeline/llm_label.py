"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

`label_tickets` caches responses and sends only validated labels to Gold.
Run `python -m scripts.bonus_llm` to check the contract. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import json
import re
from hashlib import sha256

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Accept only a JSON object with exactly one allowed string label."""
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(obj, dict) or set(obj) != {"label"}:
        return None
    label = obj["label"]
    return label if isinstance(label, str) and label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cache all answers by input/model/prompt; quarantine invalid responses."""
    model, version = llm.model, PROMPT_VERSION
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR, raw VARCHAR,
        PRIMARY KEY (input_hash, model, prompt_version))""")
    tickets = [(ticket_id, text, sha256(text.encode("utf-8")).hexdigest())
               for ticket_id, text in live_tickets(con)]
    cached = dict(con.execute(
        "SELECT input_hash, raw FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [model, version]).fetchall())
    missing = {key: text for _, text, key in tickets if key not in cached}
    estimated_tokens = estimate_tokens(list(missing.values()))
    calls_before = llm.calls
    for key, text in missing.items():
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        # Cache invalid answers too, so replay does not repeatedly call the model.
        con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?)",
                    [key, model, version, raw])
        cached[key] = raw

    rows, quarantined = [], []
    for ticket_id, _, key in tickets:
        raw = cached[key]
        label = parse_label(raw)
        if label is None:
            quarantined.append((ticket_id, key, raw, model, version,
                                'Expected JSON object with only label: bug, billing or other'))
        else:
            rows.append((ticket_id, label, model, version))
    con.execute("""CREATE OR REPLACE TABLE llm_label_quarantine (
        ticket_id VARCHAR, input_hash VARCHAR, raw VARCHAR, model VARCHAR,
        prompt_version VARCHAR, reason VARCHAR)""")
    if quarantined:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)",
                        quarantined)
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, model VARCHAR, prompt_version VARCHAR)""")
    if rows:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?)", rows)
    return {"labeled": len(rows), "quarantined": len(quarantined),
            "calls": llm.calls - calls_before, "estimated_tokens": estimated_tokens,
            "estimated_cost_usd": estimated_tokens / 1000 * PRICE_PER_1K_TOKENS_USD}
