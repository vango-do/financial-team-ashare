from __future__ import annotations

import json
import logging
import math
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MASTER_LIBRARY_ROOT = Path(os.getenv("MASTER_LIBRARY_ROOT", str(PROJECT_ROOT / "AgentLibrary")))
RAG_LOG_PATH = Path(os.getenv("MASTER_RAG_LOG_PATH", str(PROJECT_ROOT / "outputs" / "retrieval_logs.jsonl")))
DEFAULT_TOP_K = max(1, int(os.getenv("MASTER_RAG_TOP_K", "6")))
DEFAULT_MIN_SCORE = float(os.getenv("MASTER_RAG_MIN_SCORE", "0.03"))
ENABLE_RAG_QUERY_LOGGING = os.getenv("ENABLE_RAG_QUERY_LOGGING", "false").strip().lower() in {"1", "true", "yes", "on"}

logger = logging.getLogger(__name__)


AGENT_MASTER_MAP: dict[str, str] = {
    "warren_buffett_agent": "Buffett",
    "stanley_druckenmiller_agent": "Druckenmiller",
    "fundamentals_analyst_agent": "Fundamental",
    "growth_analyst_agent": "Growth",
    "peter_lynch_agent": "Lynch",
    "charlie_munger_agent": "Munger",
    "soros_agent": "Soros",
    "portfolio_manager": "ChiefAnalyst",
}


MASTER_COLLECTIONS: dict[str, str] = {
    "Buffett": "master_buffett",
    "Druckenmiller": "master_druckenmiller",
    "Fundamental": "master_fundamental",
    "Growth": "master_growth",
    "Lynch": "master_lynch",
    "Munger": "master_munger",
    "Soros": "master_soros",
    "ChiefAnalyst": "chief_analyst_memory_round",
}


MASTER_ALIASES = {
    "buffett": "Buffett",
    "druckenmiller": "Druckenmiller",
    "fundamental": "Fundamental",
    "growth": "Growth",
    "lynch": "Lynch",
    "munger": "Munger",
    "soros": "Soros",
    "chiefanalyst": "ChiefAnalyst",
}


@dataclass(slots=True)
class RetrievalHit:
    score: float
    record_id: str
    master: str
    text: str
    payload: dict[str, Any]


def resolve_master_for_agent(agent_name: str | None) -> str | None:
    if not agent_name:
        return None
    return AGENT_MASTER_MAP.get(agent_name)


def normalize_master(master: str) -> str:
    key = re.sub(r"[^a-zA-Z]", "", (master or "")).lower()
    if key in MASTER_ALIASES:
        return MASTER_ALIASES[key]
    raise ValueError(f"未知大师标识: {master}")


def collection_for_master(master: str) -> str:
    canonical = normalize_master(master)
    return MASTER_COLLECTIONS[canonical]


def _jsonl_read(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


@lru_cache(maxsize=32)
def load_master_records(master: str) -> list[dict[str, Any]]:
    canonical = normalize_master(master)
    if canonical == "ChiefAnalyst":
        return []
    path = MASTER_LIBRARY_ROOT / canonical / "memory" / "pass1_records.jsonl"
    return _jsonl_read(path)


def _tokenize(text: str) -> list[str]:
    value = str(text or "").strip().lower()
    if not value:
        return []
    alnum_tokens = [tok for tok in re.split(r"[^0-9a-zA-Z\u4e00-\u9fff]+", value) if tok]
    cn = [ch for ch in value if "\u4e00" <= ch <= "\u9fff"]
    cn_bigrams = [cn[i] + cn[i + 1] for i in range(len(cn) - 1)]
    return alnum_tokens + cn_bigrams


def _score(query: str, doc: str) -> float:
    q = _tokenize(query)
    d = _tokenize(doc)
    if not q or not d:
        return 0.0
    freq: dict[str, int] = {}
    for tok in d:
        freq[tok] = freq.get(tok, 0) + 1
    overlap = sum(1 for tok in q if tok in freq)
    weighted = sum(min(freq.get(tok, 0), 3) for tok in set(q))
    density = overlap / max(1, len(set(q)))
    length_penalty = math.sqrt(max(1, len(d)))
    return (weighted + density * 4.0) / length_penalty


def _record_text(record: dict[str, Any]) -> str:
    pass1 = str(record.get("pass1_text") or "").strip()
    if pass1:
        return pass1
    sections = record.get("sections") if isinstance(record.get("sections"), dict) else {}
    if sections:
        merged = "\n".join(str(v or "") for v in sections.values())
        if merged.strip():
            return merged
    title = str(record.get("sample_title") or "")
    tags = record.get("tags")
    tags_text = " ".join(str(x) for x in tags) if isinstance(tags, list) else str(tags or "")
    return f"{title}\n{tags_text}".strip()


def retrieve(
    *,
    master: str,
    query: str,
    top_k: int = DEFAULT_TOP_K,
    min_score: float = DEFAULT_MIN_SCORE,
    source_records: list[dict[str, Any]] | None = None,
) -> tuple[list[RetrievalHit], list[str]]:
    canonical = normalize_master(master)
    top_k = max(1, int(top_k))
    rows = source_records if source_records is not None else load_master_records(canonical)

    hits: list[RetrievalHit] = []
    dropped_cross_master_ids: list[str] = []
    for idx, row in enumerate(rows):
        record_id = str(row.get("record_id") or f"row_{idx + 1}")
        raw_row_master = row.get("master")
        if raw_row_master is None or str(raw_row_master).strip() == "":
            dropped_cross_master_ids.append(record_id)
            logger.warning("RAG record missing `master`, dropped: expected=%s record_id=%s", canonical, record_id)
            continue
        try:
            row_master = normalize_master(str(raw_row_master))
        except Exception:
            dropped_cross_master_ids.append(record_id)
            logger.warning("RAG record has invalid `master`, dropped: expected=%s record_id=%s master=%s", canonical, record_id, raw_row_master)
            continue
        if row_master != canonical:
            dropped_cross_master_ids.append(record_id)
            continue
        text = _record_text(row)
        score = _score(query=query, doc=text)
        if score < min_score:
            continue
        hits.append(
            RetrievalHit(
                score=score,
                record_id=record_id,
                master=row_master,
                text=text,
                payload=row,
            )
        )

    hits.sort(key=lambda x: x.score, reverse=True)
    return hits[:top_k], dropped_cross_master_ids


def build_context(hits: list[RetrievalHit], max_chars: int = 5200) -> str:
    blocks: list[str] = []
    used = 0
    for idx, hit in enumerate(hits, start=1):
        title = str(hit.payload.get("sample_title") or hit.payload.get("record_id") or hit.record_id)
        summary = hit.text.strip().replace("\r", "\n")
        if len(summary) > 700:
            summary = summary[:700] + "..."
        block = (
            f"[证据{idx}] id={hit.record_id} score={hit.score:.4f} master={hit.master}\n"
            f"标题: {title}\n{summary}"
        )
        if used + len(block) > max_chars:
            break
        blocks.append(block)
        used += len(block)
    return "\n\n".join(blocks)


def _query_preview(query: str, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(query or "")).strip()
    return text[:limit] + ("..." if len(text) > limit else "")


def _sanitize_event_for_disk(event: dict[str, Any]) -> dict[str, Any]:
    if ENABLE_RAG_QUERY_LOGGING:
        return dict(event)
    return {
        "master": event.get("master"),
        "top_k": event.get("top_k"),
        "hit_ids": list(event.get("hit_ids") or []),
    }


def log_retrieval_event(state: dict[str, Any] | None, event: dict[str, Any]) -> None:
    if state is not None:
        data = state.setdefault("data", {})
        logs = data.setdefault("retrieval_logs", [])
        logs.append(event)

    try:
        RAG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        disk_event = _sanitize_event_for_disk(event)
        with RAG_LOG_PATH.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(disk_event, ensure_ascii=False, indent=2, default=str))
            f.write("\n")
    except Exception:
        # Retrieval should never break main flow.
        pass


def prepare_chief_memory_records(
    *,
    tickers: list[str],
    analyst_signals: dict[str, Any],
) -> list[dict[str, Any]]:
    try:
        from src.utils.analysts import get_agent_display_name
    except Exception:
        get_agent_display_name = lambda name: name  # type: ignore[assignment]

    records: list[dict[str, Any]] = []
    for ticker in tickers:
        for agent_name, signal_map in analyst_signals.items():
            if agent_name == "risk_management_agent":
                continue
            payload = signal_map.get(ticker) if isinstance(signal_map, dict) else None
            if not isinstance(payload, dict):
                continue
            signal = str(payload.get("signal") or "").lower()
            confidence = payload.get("confidence")
            reasoning = payload.get("reasoning")
            reasoning_text = (
                json.dumps(reasoning, ensure_ascii=False, indent=2)
                if isinstance(reasoning, (dict, list))
                else str(reasoning or "")
            )
            display_name = get_agent_display_name(agent_name)
            text = (
                f"代码: {ticker}\n"
                f"角色: {display_name}\n"
                f"信号: {signal}\n"
                f"置信度: {confidence}\n"
                f"核心逻辑: {reasoning_text}"
            )
            records.append(
                {
                    "record_id": f"chief_{ticker}_{agent_name}",
                    "master": "ChiefAnalyst",
                    "sample_title": f"{ticker}-{display_name}",
                    "decision_type": signal,
                    "decision_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "pass1_text": text,
                    "tags": [ticker, str(display_name), str(signal)],
                }
            )
    return records


def retrieve_for_agent_call(
    *,
    state: dict[str, Any] | None,
    agent_name: str | None,
    query: str,
    top_k: int = DEFAULT_TOP_K,
) -> dict[str, Any]:
    master = resolve_master_for_agent(agent_name)
    if not master:
        return {
            "enabled": False,
            "master": None,
            "collection": None,
            "context": "",
            "hits": [],
            "hit_ids": [],
            "insufficient": True,
            "event": None,
        }

    source_records: list[dict[str, Any]] | None = None
    if master == "ChiefAnalyst":
        if state and isinstance(state.get("data"), dict):
            source_records = state["data"].get("chief_memory_records") or []
        else:
            source_records = []

    hits, dropped = retrieve(master=master, query=query, top_k=top_k, source_records=source_records)
    context = build_context(hits)
    hit_ids = [h.record_id for h in hits]
    event = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "call_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "agent": agent_name,
        "master": master,
        "collection": collection_for_master(master),
        "top_k": top_k,
        "hit_ids": hit_ids,
        "dropped_cross_master_ids": dropped,
        "hit_count": len(hit_ids),
        "insufficient": len(hit_ids) == 0,
    }
    if ENABLE_RAG_QUERY_LOGGING:
        event["query"] = _query_preview(query)
    else:
        event["query_redacted"] = True
    log_retrieval_event(state=state, event=event)
    return {
        "enabled": True,
        "master": master,
        "collection": collection_for_master(master),
        "context": context,
        "hits": hits,
        "hit_ids": hit_ids,
        "insufficient": len(hit_ids) == 0,
        "event": event,
    }


def isolation_audit(retrieval_logs: list[dict[str, Any]] | None) -> dict[str, Any]:
    logs = retrieval_logs or []
    polluted = [
        log
        for log in logs
        if isinstance(log, dict) and (log.get("dropped_cross_master_ids") or [])
    ]
    return {
        "total_calls": len(logs),
        "cross_master_drop_events": len(polluted),
        "is_clean": len(polluted) == 0,
        "events": polluted,
    }
