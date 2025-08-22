"""
Redis-based strategy cache for optimization artifacts.
Replaces the old in-memory cache_manager with Redis implementation.
"""

import structlog
from typing import Any, Dict, Optional, Callable
from datetime import datetime, timedelta
from storage.redis import get_redis_cache


class StrategyCache:
    """Redis-based cache for strategy configurations and timestamps."""
    
    def __init__(self):
        """Initialize strategy cache using global Redis connection."""
        self.cache = get_redis_cache()
        self.logger = structlog.get_logger()
        
        # Cache key prefixes
        self.PREFIX_CONFIG = "strategy:config:"
        self.PREFIX_TIMESTAMP = "strategy:timestamp:"
        self.PREFIX_MODEL = "strategy:model:"
        self.PREFIX_SCALER = "strategy:scaler:"
        
        # Default TTL for different types of data
        self.TTL_CONFIG = 3600  # 1 hour for configs
        self.TTL_TIMESTAMP = 86400  # 24 hours for timestamps
        self.TTL_MODEL = 7200  # 2 hours for models
    
    def get_last_run_timestamp_with_cache(self, loader_func: Callable[[], Optional[datetime]]) -> Optional[datetime]:
        """
        Get last run timestamp with caching support.
        
        Args:
            loader_func: Function to load timestamp if not cached
            
        Returns:
            Last run timestamp or None
        """
        cache_key = f"{self.PREFIX_TIMESTAMP}last_run"
        
        try:
            # Try to get from cache first
            cached_timestamp = self.cache.get(cache_key)
            if cached_timestamp is not None:
                self.logger.debug("Retrieved last run timestamp from Redis cache")
                return cached_timestamp
            
            # Not in cache, use loader function
            timestamp = loader_func()
            if timestamp is not None:
                # Cache the result
                self.cache.set(cache_key, timestamp, self.TTL_TIMESTAMP)
                self.logger.debug("Cached last run timestamp in Redis")
            
            return timestamp
            
        except Exception as e:
            self.logger.error(f"Error in timestamp caching: {e}")
            # Fallback to loader function
            return loader_func()
    
    def set_cached_last_run_timestamp(self, timestamp: datetime) -> bool:
        """
        Set/update the cached last run timestamp.
        
        Args:
            timestamp: Timestamp to cache
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = f"{self.PREFIX_TIMESTAMP}last_run"
        
        try:
            result = self.cache.set(cache_key, timestamp, self.TTL_TIMESTAMP)
            if result:
                self.logger.debug(f"Updated cached last run timestamp: {timestamp}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to cache timestamp: {e}")
            return False
    
    def get_cached_config(self, config_version: str) -> Optional[Dict]:
        """
        Get cached strategy configuration by version.
        
        Args:
            config_version: Configuration version identifier
            
        Returns:
            Cached configuration or None
        """
        cache_key = f"{self.PREFIX_CONFIG}{config_version}"
        
        try:
            config = self.cache.get(cache_key)
            if config is not None:
                self.logger.debug(f"Retrieved config version {config_version} from Redis cache")
            return config
        except Exception as e:
            self.logger.error(f"Failed to get cached config: {e}")
            return None
    
    def set_cached_config(self, config_version: str, config_data: Dict) -> bool:
        """
        Cache strategy configuration by version.
        
        Args:
            config_version: Configuration version identifier
            config_data: Configuration data to cache
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = f"{self.PREFIX_CONFIG}{config_version}"
        
        try:
            result = self.cache.set(cache_key, config_data, self.TTL_CONFIG)
            if result:
                self.logger.debug(f"Cached config version {config_version}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to cache config: {e}")
            return False
    
    def get_cached_model(self, model_path: str) -> Optional[Any]:
        """
        Get cached model by path.
        
        Args:
            model_path: Model file path identifier
            
        Returns:
            Cached model or None
        """
        cache_key = f"{self.PREFIX_MODEL}{model_path}"
        
        try:
            model = self.cache.get(cache_key)
            if model is not None:
                self.logger.debug(f"Retrieved model {model_path} from Redis cache")
            return model
        except Exception as e:
            self.logger.error(f"Failed to get cached model: {e}")
            return None
    
    def set_cached_model(self, model_path: str, model_data: Any) -> bool:
        """
        Cache model by path.
        
        Args:
            model_path: Model file path identifier
            model_data: Model data to cache
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = f"{self.PREFIX_MODEL}{model_path}"
        
        try:
            result = self.cache.set(cache_key, model_data, self.TTL_MODEL)
            if result:
                self.logger.debug(f"Cached model {model_path}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to cache model: {e}")
            return False
    
    def get_cached_scaler(self, scaler_path: str) -> Optional[Any]:
        """
        Get cached scaler by path.
        
        Args:
            scaler_path: Scaler file path identifier
            
        Returns:
            Cached scaler or None
        """
        cache_key = f"{self.PREFIX_SCALER}{scaler_path}"
        
        try:
            scaler = self.cache.get(cache_key)
            if scaler is not None:
                self.logger.debug(f"Retrieved scaler {scaler_path} from Redis cache")
            return scaler
        except Exception as e:
            self.logger.error(f"Failed to get cached scaler: {e}")
            return None
    
    def set_cached_scaler(self, scaler_path: str, scaler_data: Any) -> bool:
        """
        Cache scaler by path.
        
        Args:
            scaler_path: Scaler file path identifier
            scaler_data: Scaler data to cache
            
        Returns:
            True if successful, False otherwise
        """
        cache_key = f"{self.PREFIX_SCALER}{scaler_path}"
        
        try:
            result = self.cache.set(cache_key, scaler_data, self.TTL_MODEL)
            if result:
                self.logger.debug(f"Cached scaler {scaler_path}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to cache scaler: {e}")
            return False
    
    def clear_all_caches(self) -> Dict[str, int]:
        """
        Clear all strategy-related caches.
        
        Returns:
            Dictionary with count of cleared items by type
        """
        cleared = {}
        
        try:
            # Clear different cache types
            cleared['configs'] = self.cache.clear_pattern(f"{self.PREFIX_CONFIG}*")
            cleared['timestamps'] = self.cache.clear_pattern(f"{self.PREFIX_TIMESTAMP}*")
            cleared['models'] = self.cache.clear_pattern(f"{self.PREFIX_MODEL}*")
            cleared['scalers'] = self.cache.clear_pattern(f"{self.PREFIX_SCALER}*")
            
            total_cleared = sum(cleared.values())
            self.logger.info(f"Cleared {total_cleared} strategy cache items")
            
        except Exception as e:
            self.logger.error(f"Failed to clear caches: {e}")
        
        return cleared
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            # Get Redis stats
            redis_stats = self.cache.get_stats()
            
            # Count keys by prefix
            key_counts = {}
            for prefix, name in [
                (self.PREFIX_CONFIG, 'configs'),
                (self.PREFIX_TIMESTAMP, 'timestamps'),
                (self.PREFIX_MODEL, 'models'),
                (self.PREFIX_SCALER, 'scalers')
            ]:
                try:
                    keys = self.cache.connection.keys(f"{prefix}*")
                    key_counts[name] = len(keys) if keys else 0
                except:
                    key_counts[name] = 0
            
            return {
                'redis_stats': redis_stats,
                'strategy_cache_counts': key_counts,
                'cache_efficiency': self._calculate_cache_efficiency(redis_stats)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {}
    
    def _calculate_cache_efficiency(self, redis_stats: Dict) -> float:
        """Calculate cache hit ratio."""
        try:
            hits = redis_stats.get('keyspace_hits', 0)
            misses = redis_stats.get('keyspace_misses', 0)
            total = hits + misses
            
            if total > 0:
                return (hits / total) * 100.0
            return 0.0
        except:
            return 0.0


# Global strategy cache instance
_strategy_cache: Optional[StrategyCache] = None


def get_strategy_cache() -> StrategyCache:
    """Get the global strategy cache instance."""
    global _strategy_cache
    if _strategy_cache is None:
        _strategy_cache = StrategyCache()
    return _strategy_cache
