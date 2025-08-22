import structlog
import sys
import redis
from typing import Dict, Any, Optional
import json
import pickle
from datetime import datetime, timedelta


class RedisConnection(object):
    """Redis connection singleton following worker-py pattern."""
    _instance = None

    def __new__(cls, configuration=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            if configuration is None:
                structlog.get_logger().error("configuration is None, exiting application")
                sys.exit(3)
            cls._instance.config = configuration
            
            # Setting up the redis global connection
            connection = redis.Redis(
                host=configuration["redis"]["host"],
                port=int(configuration["redis"]["port"]),
                decode_responses=True
            )
            
            cls._instance.connection = connection
            cls._instance.logger = structlog.get_logger()
        
        return cls._instance
    
    def get_connection(self) -> redis.Redis:
        """Get the Redis connection instance."""
        return self.connection


class RedisCache:
    """Redis-based cache implementation for optimization artifacts."""
    
    def __init__(self, configuration: Dict = None, default_ttl_seconds: int = 86400):
        """
        Initialize Redis cache with configuration.
        
        Args:
            configuration: Configuration dictionary containing Redis settings
            default_ttl_seconds: Default cache expiration time in seconds (24 hours)
        """
        if configuration is None:
            raise ValueError("Configuration must be provided")
            
        self.redis_conn = RedisConnection(configuration)
        self.connection = self.redis_conn.get_connection()
        self.default_ttl = default_ttl_seconds
        self.logger = structlog.get_logger()
        
        # Test connection
        try:
            self.connection.ping()
            self.logger.info("Redis connection established successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """
        Set a value in Redis cache with optional TTL.
        
        Args:
            key: Cache key
            value: Value to cache (will be serialized)
            ttl_seconds: Time to live in seconds (uses default if None)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            ttl = ttl_seconds or self.default_ttl
            
            # Handle simple types with JSON metadata, complex types with direct pickle bytes
            if isinstance(value, (dict, list)):
                try:
                    # Try JSON first for simple dicts/lists
                    serialized_value = json.dumps(value)
                    cache_data = {
                        "value": serialized_value,
                        "type": "json",
                        "timestamp": datetime.now().isoformat()
                    }
                    result = self.connection.setex(key, ttl, json.dumps(cache_data))
                except (TypeError, ValueError):
                    # Fallback to direct pickle bytes for complex dicts/lists
                    pickled_data = pickle.dumps(value)
                    result = self.connection.setex(key + ":pickle", ttl, pickled_data)
                    
            elif isinstance(value, str):
                cache_data = {
                    "value": value,
                    "type": "string",
                    "timestamp": datetime.now().isoformat()
                }
                result = self.connection.setex(key, ttl, json.dumps(cache_data))
                
            elif isinstance(value, (int, float)):
                cache_data = {
                    "value": str(value),
                    "type": "number",
                    "timestamp": datetime.now().isoformat()
                }
                result = self.connection.setex(key, ttl, json.dumps(cache_data))
                
            elif isinstance(value, datetime):
                cache_data = {
                    "value": value.isoformat(),
                    "type": "datetime",
                    "timestamp": datetime.now().isoformat()
                }
                result = self.connection.setex(key, ttl, json.dumps(cache_data))
                
            else:
                # Use direct pickle bytes for complex objects (no hex encoding)
                pickled_data = pickle.dumps(value)
                result = self.connection.setex(key + ":pickle", ttl, pickled_data)
            
            if result:
                self.logger.debug(f"Cached key '{key}' with TTL {ttl}s")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to set cache key '{key}': {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from Redis cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        try:
            # Try normal key first (JSON metadata format)
            cached_data = self.connection.get(key)
            
            if cached_data is not None:
                # Parse JSON-serialized cached data with metadata
                cache_info = json.loads(cached_data)
                value_type = cache_info["type"]
                serialized_value = cache_info["value"]
                
                # Deserialize based on type
                if value_type == "json":
                    return json.loads(serialized_value)
                elif value_type == "string":
                    return serialized_value
                elif value_type == "number":
                    return float(serialized_value) if '.' in serialized_value else int(serialized_value)
                elif value_type == "datetime":
                    return datetime.fromisoformat(serialized_value)
                else:
                    return serialized_value
            
            # If not found, try pickle key (direct bytes format)
            pickled_data = self.connection.get(key + ":pickle")
            if pickled_data is not None:
                # Direct pickle bytes format
                return pickle.loads(pickled_data)
            
            return None
                
        except Exception as e:
            self.logger.error(f"Failed to get cache key '{key}': {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from Redis cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted, False otherwise
        """
        try:
            # Delete both normal key and potential pickle key
            result1 = self.connection.delete(key)
            result2 = self.connection.delete(key + ":pickle")
            
            total_deleted = result1 + result2
            if total_deleted > 0:
                self.logger.debug(f"Deleted cache key '{key}' ({total_deleted} keys total)")
            return total_deleted > 0
        except Exception as e:
            self.logger.error(f"Failed to delete cache key '{key}': {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in Redis cache.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists, False otherwise
        """
        try:
            # Check both normal key and pickle key
            return bool(self.connection.exists(key) or self.connection.exists(key + ":pickle"))
        except Exception as e:
            self.logger.error(f"Failed to check cache key '{key}': {e}")
            return False
    
    def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching a pattern.
        
        Args:
            pattern: Redis key pattern (e.g., "config:*")
            
        Returns:
            Number of keys deleted
        """
        try:
            # Get both normal keys and pickle keys matching the pattern
            keys = self.connection.keys(pattern)
            pickle_keys = self.connection.keys(pattern + ":pickle")
            all_keys = keys + pickle_keys
            
            if all_keys:
                deleted = self.connection.delete(*all_keys)
                self.logger.info(f"Deleted {deleted} keys matching pattern '{pattern}' (including pickle keys)")
                return deleted
            return 0
        except Exception as e:
            self.logger.error(f"Failed to clear pattern '{pattern}': {e}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            info = self.connection.info()
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "total_keys": sum(info.get(f"db{i}", {}).get("keys", 0) for i in range(16))
            }
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {}


# Global cache instance - will be initialized by the application
_redis_cache: Optional[RedisCache] = None


def get_redis_cache() -> RedisCache:
    """Get the global Redis cache instance."""
    global _redis_cache
    if _redis_cache is None:
        raise RuntimeError("Redis cache not initialized. Call init_redis_cache() first.")
    return _redis_cache


def init_redis_cache(configuration: Dict) -> RedisCache:
    """Initialize the global Redis cache instance."""
    global _redis_cache
    _redis_cache = RedisCache(configuration)
    return _redis_cache
