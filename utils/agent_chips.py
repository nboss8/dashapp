"""Load suggested questions from semantic model YAML for agent chat chips."""
import os
from pathlib import Path

import yaml

_CHIP_QUESTIONS: list[str] | None = None


def get_suggested_questions(limit: int = 6) -> list[str]:
    """Load suggested questions from pallet_inventory_trends.yaml verified_queries."""
    global _CHIP_QUESTIONS
    if _CHIP_QUESTIONS is not None:
        return _CHIP_QUESTIONS
    path = Path(__file__).resolve().parent.parent / "semantic_models" / "pallet_inventory_trends.yaml"
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            queries = data.get("verified_queries") or []
            onboarding = [q["question"] for q in queries if q.get("use_as_onboarding_question")]
            rest = [q["question"] for q in queries if not q.get("use_as_onboarding_question")]
            _CHIP_QUESTIONS = (onboarding + rest)[:limit]
        else:
            _CHIP_QUESTIONS = [
                "What is total on-hand inventory in cartons?",
                "Show on-hand cartons by variety.",
                "What are today packed shipped and staged totals?",
            ]
    except Exception:
        _CHIP_QUESTIONS = [
            "What is total on-hand inventory in cartons?",
            "Show on-hand by variety.",
        ]
    return _CHIP_QUESTIONS
