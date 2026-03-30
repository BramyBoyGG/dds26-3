"""
Shared Redis connection factory using Redis Sentinel for automatic failover.

Connections go through Sentinel, which discovers the current master for each
named Redis instance.  The REDIS_SENTINELS environment variable must be set
as a comma-separated list of host:port pairs.
"""

import os
import redis
from redis.sentinel import Sentinel


_sentinel_instance: Sentinel | None = None


def _get_sentinel() -> Sentinel:
    """Return a shared Sentinel instance (created on first call)."""
    global _sentinel_instance
    if _sentinel_instance is not None:
        return _sentinel_instance

    sentinels_raw = os.environ["REDIS_SENTINELS"]

    sentinel_list = []
    for entry in sentinels_raw.split(","):
        entry = entry.strip()
        host, port = entry.rsplit(":", 1)
        sentinel_list.append((host, int(port)))

    _sentinel_instance = Sentinel(
        sentinel_list,
        sentinel_kwargs={"password": None},
        socket_timeout=10.0,
    )
    return _sentinel_instance


def get_redis_connection(
    master_name_env: str,
    password_env: str,
) -> redis.Redis:
    """
    Create a Redis connection via Sentinel discovery.

    Args:
        master_name_env: Env var name holding the Sentinel master name
        password_env: Env var name holding the Redis password

    Returns:
        A redis.Redis (SentinelManagedConnection) instance.
    """
    sentinel = _get_sentinel()
    master_name = os.environ[master_name_env]
    password = os.environ.get(password_env, "redis")

    return sentinel.master_for(
        master_name,
        password=password,
        retry_on_timeout=True,
        socket_timeout=10.0,
    )
