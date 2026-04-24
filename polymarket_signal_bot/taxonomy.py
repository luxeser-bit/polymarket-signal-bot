from __future__ import annotations

import re

from .models import Signal, Trade


SPORTS_TERMS = {
    "nba",
    "nfl",
    "nhl",
    "mlb",
    "ufc",
    "soccer",
    "football",
    "champions league",
    "premier league",
    "spread:",
    "o/u",
    "vs.",
    "thunder",
    "knicks",
    "hawks",
    "cavaliers",
    "raptors",
}

POLITICS_TERMS = {
    "trump",
    "biden",
    "election",
    "senate",
    "congress",
    "president",
    "tariff",
    "fed",
    "powell",
    "ukraine",
    "russia",
    "israel",
    "france",
    "vote",
    "poll",
}

CRYPTO_TERMS = {
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "xrp",
    "doge",
    "crypto",
    "binance",
}

BUSINESS_TERMS = {
    "apple",
    "tesla",
    "nvidia",
    "openai",
    "ipo",
    "earnings",
    "stock",
    "s&p",
    "nasdaq",
    "rate cut",
    "cpi",
    "gdp",
}

CULTURE_TERMS = {
    "grammy",
    "oscar",
    "movie",
    "album",
    "song",
    "stream",
    "tiktok",
    "youtube",
    "x post",
    "tweet",
}


def market_category(value: Trade | Signal | str) -> str:
    text = _text(value)
    if not text:
        return "other"
    scores = {
        "sports": _score(text, SPORTS_TERMS),
        "politics": _score(text, POLITICS_TERMS),
        "crypto": _score(text, CRYPTO_TERMS),
        "business": _score(text, BUSINESS_TERMS),
        "culture": _score(text, CULTURE_TERMS),
    }
    category, score = max(scores.items(), key=lambda item: item[1])
    return category if score > 0 else "other"


def market_category_label(category: str) -> str:
    return {
        "sports": "SPORTS",
        "politics": "POLITICS",
        "crypto": "CRYPTO",
        "business": "BUSINESS",
        "culture": "CULTURE",
        "other": "OTHER",
    }.get(category, "OTHER")


def _text(value: Trade | Signal | str) -> str:
    if isinstance(value, str):
        return value.lower()
    parts = [
        getattr(value, "title", ""),
        getattr(value, "slug", ""),
        getattr(value, "event_slug", ""),
        getattr(value, "outcome", ""),
    ]
    return " ".join(str(part) for part in parts if part).lower()


def _score(text: str, terms: set[str]) -> int:
    score = 0
    for term in terms:
        if " " in term or "/" in term or ":" in term or "." in term:
            if term in text:
                score += 1
        elif re.search(rf"\b{re.escape(term)}\b", text):
            score += 1
    return score
