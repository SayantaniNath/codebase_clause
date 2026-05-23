"""
fraud_detector.py
-----------------
Rule-based fraud detection engine for real-time transaction/user analysis.

This module evaluates each user record against behavioral signals and assigns
a fraud risk score. It integrates directly into the Kafka consumer pipeline
so every record is scored as it arrives.

Risk Levels:
  CLEAN       (score  0–30) — no significant anomalies
  SUSPICIOUS  (score 31–60) — worth monitoring; queue for analyst review
  HIGH_RISK   (score  61+)  — immediate review needed; alert published

Signals evaluated (each contributes a fixed weight to the total score):
  UNVERIFIED_HIGH_SPEND      — unverified account with substantial monthly spend
  NO_2FA_ENTERPRISE          — enterprise plan without two-factor authentication
  ZERO_LOGINS_RECENT_PURCHASE — purchase recorded but account has never logged in
  LOW_ENGAGEMENT_HIGH_SPEND  — very low engagement score paired with high spend
  RISK_TAG_HIGH_SPEND        — at-risk/churned metadata tag + high spend
  INACTIVE_RECENT_PURCHASE   — inactive account status but purchase within 14 days
  LARGE_PURCHASE             — single purchase exceeds high-value threshold ($400)
"""

import json
import logging
from datetime import datetime
from typing import TypedDict

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Risk thresholds — score boundaries for each tier
# ---------------------------------------------------------------------------
SCORE_CLEAN_MAX = 30
SCORE_SUSPICIOUS_MAX = 60

# ---------------------------------------------------------------------------
# Signal weights — points added to score when a rule fires
# ---------------------------------------------------------------------------
WEIGHT_UNVERIFIED_HIGH_SPEND = 25
WEIGHT_NO_2FA_ENTERPRISE = 20
WEIGHT_ZERO_LOGINS_PURCHASE = 30
WEIGHT_LOW_ENGAGEMENT_HIGH_SPEND = 20
WEIGHT_RISK_TAG_HIGH_SPEND = 15
WEIGHT_INACTIVE_RECENT_PURCHASE = 20
WEIGHT_LARGE_PURCHASE = 15


# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

class FraudSignal(TypedDict):
    rule: str
    weight: int
    detail: str


class FraudResult(TypedDict):
    user_id: str
    score: int
    risk_level: str   # "CLEAN" | "SUSPICIOUS" | "HIGH_RISK"
    signals: list     # list[FraudSignal]
    evaluated_at: str  # ISO-8601 UTC timestamp


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_metadata(record: dict) -> dict:
    """Returns the metadata sub-dict, handling both dict and JSON-string forms."""
    meta = record.get("metadata") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (json.JSONDecodeError, ValueError):
            meta = {}
    return meta


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


# ---------------------------------------------------------------------------
# Core scoring function
# ---------------------------------------------------------------------------

def score_record(record: dict) -> FraudResult:
    """
    Evaluates a single user record against all fraud detection rules.

    Returns a FraudResult containing:
      - score       : integer 0–100
      - risk_level  : "CLEAN", "SUSPICIOUS", or "HIGH_RISK"
      - signals     : list of triggered rules with weights and detail strings
      - evaluated_at: ISO-8601 UTC timestamp of when scoring ran
    """
    signals: list[FraudSignal] = []
    score = 0

    # --- Parse nested fields ---
    meta = _parse_metadata(record)
    scores_meta = meta.get("scores") or {}
    last_purchase = meta.get("last_purchase") or {}
    tags: list = meta.get("tags") or []

    is_verified = _to_bool(record.get("is_verified", True))
    two_factor_enabled = _to_bool(record.get("two_factor_enabled", True))
    monthly_spend = _to_float(record.get("monthly_spend", 0))
    login_count = _to_int(record.get("login_count", 0))
    subscription_plan = str(record.get("subscription_plan", "")).lower()
    status = str(record.get("status", "")).lower()
    engagement = _to_float(scores_meta.get("engagement", 5.0))
    purchase_amount = _to_float(last_purchase.get("amount", 0))
    purchase_days_ago = _to_int(last_purchase.get("days_ago", 365))

    # --- Rule 1: Unverified account with high spend ---
    # An unverified identity spending heavily is a primary fraud signal.
    if not is_verified and monthly_spend > 20.0:
        w = WEIGHT_UNVERIFIED_HIGH_SPEND
        score += w
        signals.append({
            "rule": "UNVERIFIED_HIGH_SPEND",
            "weight": w,
            "detail": f"Account not email-verified; monthly_spend=${monthly_spend:.2f}",
        })

    # --- Rule 2: Enterprise plan without 2FA ---
    # High-value accounts without 2FA are prime account-takeover targets.
    if subscription_plan == "enterprise" and not two_factor_enabled:
        w = WEIGHT_NO_2FA_ENTERPRISE
        score += w
        signals.append({
            "rule": "NO_2FA_ENTERPRISE",
            "weight": w,
            "detail": "Enterprise account missing two-factor authentication",
        })

    # --- Rule 3: No logins on record but recent large purchase ---
    # Indicates possible account takeover — attacker purchases but doesn't log in normally.
    if login_count == 0 and purchase_amount > 100.0 and purchase_days_ago <= 30:
        w = WEIGHT_ZERO_LOGINS_PURCHASE
        score += w
        signals.append({
            "rule": "ZERO_LOGINS_RECENT_PURCHASE",
            "weight": w,
            "detail": (
                f"login_count=0 but purchase of ${purchase_amount:.2f} "
                f"recorded {purchase_days_ago}d ago"
            ),
        })

    # --- Rule 4: Very low engagement paired with high spend ---
    # Legitimate users who spend heavily typically show high engagement.
    if engagement < 2.5 and monthly_spend > 50.0:
        w = WEIGHT_LOW_ENGAGEMENT_HIGH_SPEND
        score += w
        signals.append({
            "rule": "LOW_ENGAGEMENT_HIGH_SPEND",
            "weight": w,
            "detail": f"engagement={engagement:.1f}/10 but monthly_spend=${monthly_spend:.2f}",
        })

    # --- Rule 5: At-risk/churned tag combined with high spend ---
    # Churn-tagged users making large purchases may indicate stolen credentials.
    risk_tags = {"at-risk", "churned"}
    triggered = risk_tags.intersection(set(tags))
    if triggered and monthly_spend > 30.0:
        w = WEIGHT_RISK_TAG_HIGH_SPEND
        score += w
        signals.append({
            "rule": "RISK_TAG_HIGH_SPEND",
            "weight": w,
            "detail": f"Tags {sorted(triggered)} present; monthly_spend=${monthly_spend:.2f}",
        })

    # --- Rule 6: Inactive account status with very recent purchase ---
    # Deactivated accounts should not have recent transaction activity.
    if status == "inactive" and purchase_days_ago <= 14:
        w = WEIGHT_INACTIVE_RECENT_PURCHASE
        score += w
        signals.append({
            "rule": "INACTIVE_RECENT_PURCHASE",
            "weight": w,
            "detail": f"Account inactive but purchase {purchase_days_ago}d ago",
        })

    # --- Rule 7: Single purchase above high-value threshold ---
    # Flags unusually large one-time transactions for review.
    if purchase_amount > 400.0:
        w = WEIGHT_LARGE_PURCHASE
        score += w
        signals.append({
            "rule": "LARGE_PURCHASE",
            "weight": w,
            "detail": f"Single purchase=${purchase_amount:.2f} exceeds $400 threshold",
        })

    # Clamp score to [0, 100]
    score = min(score, 100)

    # Determine risk tier
    if score <= SCORE_CLEAN_MAX:
        risk_level = "CLEAN"
    elif score <= SCORE_SUSPICIOUS_MAX:
        risk_level = "SUSPICIOUS"
    else:
        risk_level = "HIGH_RISK"

    user_id = str(record.get("id", "unknown"))
    log.debug("Fraud evaluation: user_id=%s score=%d risk=%s", user_id, score, risk_level)

    return FraudResult(
        user_id=user_id,
        score=score,
        risk_level=risk_level,
        signals=signals,
        evaluated_at=datetime.utcnow().isoformat() + "Z",
    )


def is_high_risk(result: FraudResult) -> bool:
    """Returns True when the fraud result is HIGH_RISK."""
    return result["risk_level"] == "HIGH_RISK"


def is_flagged(result: FraudResult) -> bool:
    """Returns True for SUSPICIOUS or HIGH_RISK results."""
    return result["risk_level"] in ("SUSPICIOUS", "HIGH_RISK")
