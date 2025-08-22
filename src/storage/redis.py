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
            
            # Serialize value based on type
            if isinstance(value, (dict, list)):
                try:
                    serialized_value = json.dumps(value)
                    value_type = "json"
                except (TypeError, ValueError):
                    # If JSON serialization fails, use pickle
                    serialized_value = pickle.dumps(value).hex()
                    value_type = "pickle"
            elif isinstance(value, str):
                serialized_value = value
                value_type = "string"
            elif isinstance(value, (int, float)):
                serialized_value = str(value)
                value_type = "number"
            elif isinstance(value, datetime):
                serialized_value = value.isoformat()
                value_type = "datetime"
            else:
                # Use pickle for complex objects
                serialized_value = pickle.dumps(value).hex()
                value_type = "pickle"
            
            # Store with metadata
            cache_data = {
                "value": serialized_value,
                "type": value_type,
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                result = self.connection.setex(
                    key, 
                    ttl, 
                    json.dumps(cache_data)
                )
            except (TypeError, ValueError) as e:
                self.logger.error(f"JSON serialization failed for cache_data: {e}")
                # Fallback: store the entire cache_data as pickle
                serialized_cache_data = pickle.dumps(cache_data).hex()
                result = self.connection.setex(
                    key + ":pickle", 
                    ttl, 
                    serialized_cache_data
                )
            
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
            # Try normal key first
            cached_data = self.connection.get(key)
            
            # If not found, try pickle fallback key
            if cached_data is None:
                cached_data = self.connection.get(key + ":pickle")
                if cached_data is not None:
                    # This is a pickle-serialized cache_data
                    cache_info = pickle.loads(bytes.fromhex(cached_data.decode()))
                    value_type = cache_info["type"]
                    serialized_value = cache_info["value"]
                else:
                    return None
            else:
                # Parse JSON-serialized cached data
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
            elif value_type == "pickle":
                return pickle.loads(bytes.fromhex(serialized_value))
            else:
                return serialized_value
                
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
            result = self.connection.delete(key)
            if result:
                self.logger.debug(f"Deleted cache key '{key}'")
            return bool(result)
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
            return bool(self.connection.exists(key))
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
            keys = self.connection.keys(pattern)
            if keys:
                deleted = self.connection.delete(*keys)
                self.logger.info(f"Deleted {deleted} keys matching pattern '{pattern}'")
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
