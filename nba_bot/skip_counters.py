"""Skip counters — track signal filtering for dashboard and logging."""

import threading

_lock = threading.Lock()

counters: dict[str, int] = {
    "signals_seen": 0,
    "skipped_disabled": 0,
    "skipped_platform": 0,
    "skipped_sport": 0,
    "skipped_live": 0,
    "skipped_time_window": 0,
    "skipped_no_edge": 0,
    "skipped_edge_persistence": 0,
    "skipped_pinnacle_stale": 0,
    "skipped_pinnacle_frozen": 0,
    "skipped_win_prob": 0,
    "skipped_risk": 0,
    "skipped_fill_missed": 0,
    "created": 0,
    "settled_won": 0,
    "settled_lost": 0,
    "settled_other": 0,
}


def inc(key: str, n: int = 1) -> None:
    with _lock:
        counters[key] = counters.get(key, 0) + n


def get(key: str) -> int:
    return counters.get(key, 0)


def summary() -> str:
    c = counters
    settled = c["settled_won"] + c["settled_lost"] + c["settled_other"]
    skipped = sum(v for k, v in c.items() if k.startswith("skipped_"))
    return (
        f"seen={c['signals_seen']} skip={skipped} "
        f"created={c['created']} settled={settled} "
        f"(W{c['settled_won']}/L{c['settled_lost']})"
    )


def as_dict() -> dict[str, int]:
    return dict(counters)
