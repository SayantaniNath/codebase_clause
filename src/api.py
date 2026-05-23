"""
api.py
------
GraphQL API built with Strawberry + FastAPI.
Reads user data and fraud detection results from Redis.

Flow:
  Consumer (browser/app/service)
      │
      │  GraphQL query
      ▼
  FastAPI + Strawberry
      │
      │  hgetall / keys
      ▼
  Redis Cache  (user:* and fraud:* keys)

Run with:
  uvicorn src.api:app --reload --port 8000

Then open:
  http://localhost:8000/graphql  ← interactive GraphQL playground
"""

import json
import logging
import os

from dotenv import load_dotenv
load_dotenv()

import redis
import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI
from typing import Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------
REDIS_CONFIG = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "db": int(os.environ.get("REDIS_DB", 0)),
    "decode_responses": True,
}

REDIS_KEY_PREFIX = "user:"
REDIS_FRAUD_PREFIX = "fraud:"

# Create a single shared Redis client for the whole API
redis_client = redis.Redis(**REDIS_CONFIG)

# ---------------------------------------------------------------------------
# Helper — read a user from Redis
# ---------------------------------------------------------------------------

def get_user_from_redis(user_id: str) -> Optional[dict]:
    """
    Fetches a single user record from Redis by ID.
    Returns None if the user doesn't exist.
    """
    key = f"{REDIS_KEY_PREFIX}{user_id}"
    data = redis_client.hgetall(key)  # hgetall returns all fields of a hash as a dict
    return data if data else None     # return None if the key doesn't exist


def get_all_users_from_redis() -> list[dict]:
    """
    Fetches all user records from Redis.
    Uses KEYS pattern to find all user:* keys.
    """
    keys = redis_client.keys(f"{REDIS_KEY_PREFIX}*")
    users = []
    for key in keys:
        data = redis_client.hgetall(key)
        if data:
            users.append(data)
    return users


def get_fraud_result_from_redis(user_id: str) -> Optional[dict]:
    """Fetches a single fraud result by user ID. Returns None if not found."""
    key = f"{REDIS_FRAUD_PREFIX}{user_id}"
    data = redis_client.hgetall(key)
    return data if data else None


def get_all_fraud_results_from_redis(risk_level: Optional[str] = None) -> list[dict]:
    """
    Fetches all fraud results from Redis.
    If risk_level is provided, filters to that tier (CLEAN/SUSPICIOUS/HIGH_RISK).
    """
    keys = redis_client.keys(f"{REDIS_FRAUD_PREFIX}*")
    results = []
    for key in keys:
        data = redis_client.hgetall(key)
        if not data:
            continue
        if risk_level and data.get("risk_level") != risk_level:
            continue
        results.append(data)
    return results

# ---------------------------------------------------------------------------
# GraphQL Schema — defines the shape of the data
# ---------------------------------------------------------------------------

@strawberry.type
class User:
    """
    Represents a user in the GraphQL schema.
    Each field maps to a field in the Redis hash.
    """
    id: str
    name: str
    email: str
    phone: str
    age: str
    city: str
    country: str
    timezone: str
    status: str
    is_verified: str
    two_factor_enabled: str
    subscription_plan: str
    monthly_spend: str
    login_count: str
    last_login: Optional[str]
    referral_source: str
    device_type: str
    browser: str
    metadata: Optional[str]       # stored as JSON string in Redis
    created_at: str


@strawberry.type
class FraudAlert:
    """
    Represents a fraud detection result for a single user.
    Populated by the fraud_detector module and stored under "fraud:<user_id>" in Redis.

    Example GraphQL query:
      query {
        getHighRiskUsers {
          userId
          score
          riskLevel
          signals
          evaluatedAt
        }
      }
    """
    user_id: str
    score: str           # integer score 0–100 (stored as string in Redis)
    risk_level: str      # CLEAN | SUSPICIOUS | HIGH_RISK
    signal_count: str    # number of triggered rules (stored as string)
    signals: str         # JSON array of triggered signal objects
    evaluated_at: str    # ISO-8601 UTC timestamp


@strawberry.type
class Query:
    """
    Defines all the available GraphQL queries.
    Each method decorated with @strawberry.field becomes a query.
    """

    @strawberry.field
    def get_user(self, id: str) -> Optional[User]:
        """
        Fetch a single user by ID.

        Example GraphQL query:
          query {
            getUser(id: "1") {
              name
              email
              subscriptionPlan
            }
          }
        """
        data = get_user_from_redis(id)
        if not data:
            log.warning("User id=%s not found in Redis", id)
            return None
        log.info("Serving user id=%s from Redis", id)
        return User(**data)

    @strawberry.field
    def get_all_users(self) -> list[User]:
        """
        Fetch all users from Redis.

        Example GraphQL query:
          query {
            getAllUsers {
              id
              name
              email
              status
            }
          }
        """
        users = get_all_users_from_redis()
        log.info("Serving %d users from Redis", len(users))
        return [User(**u) for u in users]

    @strawberry.field
    def get_users_by_plan(self, plan: str) -> list[User]:
        """
        Fetch users filtered by subscription plan.

        Example GraphQL query:
          query {
            getUsersByPlan(plan: "pro") {
              id
              name
              monthlySpend
            }
          }
        """
        all_users = get_all_users_from_redis()
        # Filter users where subscription_plan matches the requested plan
        filtered = [u for u in all_users if u.get("subscription_plan") == plan]
        log.info("Serving %d users with plan='%s'", len(filtered), plan)
        return [User(**u) for u in filtered]

    @strawberry.field
    def get_users_by_status(self, status: str) -> list[User]:
        """
        Fetch users filtered by status (active, inactive, pending).

        Example GraphQL query:
          query {
            getUsersByStatus(status: "active") {
              id
              name
              loginCount
            }
          }
        """
        all_users = get_all_users_from_redis()
        filtered = [u for u in all_users if u.get("status") == status]
        log.info("Serving %d users with status='%s'", len(filtered), status)
        return [User(**u) for u in filtered]

    @strawberry.field
    def get_users_by_country(self, country: str) -> list[User]:
        """
        Fetch users from a specific country.

        Example GraphQL query:
          query {
            getUsersByCountry(country: "US") {
              id
              name
              city
            }
          }
        """
        all_users = get_all_users_from_redis()
        filtered = [u for u in all_users if u.get("country") == country]
        log.info("Serving %d users from country='%s'", len(filtered), country)
        return [User(**u) for u in filtered]

    # -----------------------------------------------------------------------
    # Fraud detection queries
    # -----------------------------------------------------------------------

    @strawberry.field
    def get_fraud_result(self, user_id: str) -> Optional[FraudAlert]:
        """
        Fetch the fraud detection result for a specific user.

        Example GraphQL query:
          query {
            getFraudResult(userId: "42") {
              userId
              score
              riskLevel
              signals
            }
          }
        """
        data = get_fraud_result_from_redis(user_id)
        if not data:
            log.warning("No fraud result for user_id=%s", user_id)
            return None
        log.info("Serving fraud result for user_id=%s risk=%s", user_id, data.get("risk_level"))
        return FraudAlert(**data)

    @strawberry.field
    def get_all_fraud_alerts(self) -> list[FraudAlert]:
        """
        Fetch all fraud results regardless of risk level.

        Example GraphQL query:
          query {
            getAllFraudAlerts {
              userId
              score
              riskLevel
              signalCount
            }
          }
        """
        results = get_all_fraud_results_from_redis()
        log.info("Serving %d total fraud results", len(results))
        return [FraudAlert(**r) for r in results]

    @strawberry.field
    def get_high_risk_users(self) -> list[FraudAlert]:
        """
        Fetch only HIGH_RISK fraud alerts — the users needing immediate review.

        Example GraphQL query:
          query {
            getHighRiskUsers {
              userId
              score
              signals
              evaluatedAt
            }
          }
        """
        results = get_all_fraud_results_from_redis(risk_level="HIGH_RISK")
        log.info("Serving %d HIGH_RISK fraud alerts", len(results))
        return [FraudAlert(**r) for r in results]

    @strawberry.field
    def get_suspicious_users(self) -> list[FraudAlert]:
        """
        Fetch SUSPICIOUS fraud alerts — users worth monitoring but not yet high risk.

        Example GraphQL query:
          query {
            getSuspiciousUsers {
              userId
              score
              riskLevel
              signals
            }
          }
        """
        results = get_all_fraud_results_from_redis(risk_level="SUSPICIOUS")
        log.info("Serving %d SUSPICIOUS fraud alerts", len(results))
        return [FraudAlert(**r) for r in results]


# ---------------------------------------------------------------------------
# FastAPI app + GraphQL route
# ---------------------------------------------------------------------------

# Create the GraphQL schema from the Query class
schema = strawberry.Schema(query=Query)

# Create the FastAPI application
app = FastAPI(
    title="User Pipeline GraphQL API",
    description="Serves user data and fraud detection results from Redis via GraphQL",
    version="2.0.0",
)

# Mount the GraphQL endpoint at /graphql
# This also provides an interactive playground at /graphql in the browser
graphql_router = GraphQLRouter(schema)
app.include_router(graphql_router, prefix="/graphql")


@app.get("/")
def root():
    """Health check endpoint — confirms the API is running."""
    return {
        "status": "running",
        "graphql_endpoint": "/graphql",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    """Checks if Redis is reachable."""
    try:
        redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except redis.RedisError as exc:
        return {"status": "unhealthy", "redis": str(exc)}
