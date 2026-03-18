"""
Shared Redis connection factory that supports both direct connections and
Redis Sentinel-based discovery.

When the REDIS_SENTINELS environment variable is set (comma-separated list of
host:port pairs), connections go through Sentinel for automatic failover.
Otherwise, falls back to direct redis.Redis() connections.
"""

import os
import redis
from redis.sentinel import Sentinel


_sentinel_instance: Sentinel | None = None


def _get_sentinel() -> Sentinel | None:
    """Return a shared Sentinel instance, or None if not configured."""
    global _sentinel_instance
    if _sentinel_instance is not None:
        return _sentinel_instance

    sentinels_raw = os.environ.get("REDIS_SENTINELS")
    if not sentinels_raw:
        return None

    sentinel_list = []
    for entry in sentinels_raw.split(","):
        entry = entry.strip()
        host, port = entry.rsplit(":", 1)
        sentinel_list.append((host, int(port)))

    _sentinel_instance = Sentinel(
        sentinel_list,
        sentinel_kwargs={"password": None},
        socket_timeout=5.0,
    )
    return _sentinel_instance


def get_redis_connection(
    master_name_env: str,
    password_env: str,
    host_env: str,
    port_env: str,
    db_env: str,
) -> redis.Redis:
    """
    Create a Redis connection using Sentinel discovery or direct connect.

    Args:
        master_name_env: Env var name holding the Sentinel master name
        password_env: Env var name holding the Redis password
        host_env: Env var name holding the direct-connect host (fallback)
        port_env: Env var name holding the direct-connect port (fallback)
        db_env: Env var name holding the Redis DB number (fallback)

    Returns:
        A redis.Redis (or SentinelManagedConnection) instance.
    """
    sentinel = _get_sentinel()
    master_name = os.environ.get(master_name_env)
    password = os.environ.get(password_env, "redis")

    if sentinel and master_name:
        return sentinel.master_for(
            master_name,
            password=password,
            db=int(os.environ.get(db_env, "0")),
            retry_on_timeout=True,
            socket_timeout=5.0,
        )

    # Fallback: direct connection (e.g. Kubernetes with service discovery)
    return redis.Redis(
        host=os.environ[host_env],
        port=int(os.environ.get(port_env, "6379")),
        password=password,
        db=int(os.environ.get(db_env, "0")),
        retry_on_timeout=True,
        socket_timeout=5.0,
    )
