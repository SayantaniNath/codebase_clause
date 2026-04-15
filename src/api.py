"""
api.py
------
GraphQL API built with Strawberry + FastAPI.
Reads user data from Redis and serves it to consumers.

Flow:
  Consumer (browser/app/service)
      │
      │  GraphQL query
      ▼
  FastAPI + Strawberry
      │
      │  hgetall / keys
      ▼
  Redis Cache

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
    # keys("user:*") returns all Redis keys matching the pattern
    keys = redis_client.keys(f"{REDIS_KEY_PREFIX}*")
    users = []
    for key in keys:
        data = redis_client.hgetall(key)
        if data:
            users.append(data)
    return users

# ---------------------------------------------------------------------------
# GraphQL Schema — defines the shape of the data
# ---------------------------------------------------------------------------

@strawberry.type
class User:
    """
    Represents a user in the GraphQL schema.
    Each field here maps to a field in the Redis hash.
    @strawberry.type tells Strawberry this is a GraphQL type.
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


# ---------------------------------------------------------------------------
# FastAPI app + GraphQL route
# ---------------------------------------------------------------------------

# Create the GraphQL schema from the Query class
schema = strawberry.Schema(query=Query)

# Create the FastAPI application
app = FastAPI(
    title="User Pipeline GraphQL API",
    description="Serves user data from Redis via GraphQL",
    version="1.0.0",
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
