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
        self.PREFIX_VERSION = "strategy:version:"
        
        # Default TTL for different types of data - optimized for frequent cycles
        # Set long TTLs since cache invalidation is version-based, not time-based
        self.TTL_CONFIG = 604800  # 7 days for configs (only cleared on version change)
        self.TTL_TIMESTAMP = 86400  # 24 hours for timestamps
        self.TTL_MODEL = 604800  # 7 days for models (only cleared on version change)
        self.TTL_SCALER = 604800  # 7 days for scalers (only cleared on version change)
        self.TTL_VERSION = 86400  # 24 hours for version tracking
    
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
            result = self.cache.set(cache_key, scaler_data, self.TTL_SCALER)
            if result:
                self.logger.debug(f"Cached scaler {scaler_path}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to cache scaler: {e}")
            return False
    
    def invalidate_cached_model(self, model_path: str) -> bool:
        """
        Invalidate a specific cached model.
        
        Args:
            model_path: Path to the model file in MinIO
            
        Returns:
            True if successfully invalidated, False otherwise
        """
        cache_key = f"{self.PREFIX_MODEL}{model_path}"
        
        try:
            self.cache.delete(cache_key)
            self.logger.info(f"Invalidated cached model: {model_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to invalidate cached model: {e}")
            return False
    
    def invalidate_cached_scaler(self, scaler_path: str) -> bool:
        """
        Invalidate a specific cached scaler.
        
        Args:
            scaler_path: Path to the scaler file in MinIO
            
        Returns:
            True if successfully invalidated, False otherwise
        """
        cache_key = f"{self.PREFIX_SCALER}{scaler_path}"
        
        try:
            self.cache.delete(cache_key)
            self.logger.info(f"Invalidated cached scaler: {scaler_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to invalidate cached scaler: {e}")
            return False
    
    def invalidate_cached_config(self, config_version: str) -> bool:
        """
        Invalidate a specific cached config.
        
        Args:
            config_version: Version of the config to invalidate
            
        Returns:
            True if successfully invalidated, False otherwise
        """
        cache_key = f"{self.PREFIX_CONFIG}{config_version}"
        
        try:
            self.cache.delete(cache_key)
            self.logger.info(f"Invalidated cached config: {config_version}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to invalidate cached config: {e}")
            return False
    
    def check_version_and_invalidate_if_needed(self, current_version: str) -> bool:
        """
        Check if version has changed and invalidate cache if needed.
        
        Args:
            current_version: Current strategy configuration version
            
        Returns:
            True if cache was invalidated, False otherwise
        """
        version_key = f"{self.PREFIX_VERSION}current"
        
        try:
            cached_version = self.cache.get(version_key)
            
            if cached_version != current_version:
                self.logger.info(f"Version changed from {cached_version} to {current_version}, invalidating cache...")
                
                # Clear all strategy-related caches
                cleared_counts = self.clear_all_caches()
                
                # Set new version
                self.cache.set(version_key, current_version, self.TTL_VERSION)
                
                self.logger.info(f"Cache invalidated. Cleared {sum(cleared_counts.values())} items")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking version: {e}")
            return False
    
    def get_current_cached_version(self) -> Optional[str]:
        """Get the currently cached strategy version."""
        version_key = f"{self.PREFIX_VERSION}current"
        try:
            return self.cache.get(version_key)
        except Exception as e:
            self.logger.error(f"Error getting cached version: {e}")
            return None
    
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
            # Get current cached version
            current_version = self.get_current_cached_version()
            
            # Get last run timestamp
            timestamp_key = f"{self.PREFIX_TIMESTAMP}last_run"
            cached_timestamp = self.cache.get(timestamp_key)
            
            # Get Redis stats
            redis_stats = self.cache.get_stats()
            
            # Count keys by prefix with detailed structure
            key_counts = {}
            for prefix, name in [
                (self.PREFIX_CONFIG, 'configs'),
                (self.PREFIX_TIMESTAMP, 'timestamps'),
                (self.PREFIX_MODEL, 'models'),
                (self.PREFIX_SCALER, 'scalers'),
                (self.PREFIX_VERSION, 'versions')
            ]:
                try:
                    keys = self.cache.connection.keys(f"{prefix}*")
                    count = len(keys) if keys else 0
                    key_counts[name] = {
                        'active_items': count,
                        'expired_items': 0  # Redis handles TTL automatically
                    }
                except:
                    key_counts[name] = {
                        'active_items': 0,
                        'expired_items': 0
                    }
            
            # Format redis stats properly
            formatted_redis_stats = {
                'active_items': redis_stats.get('used_memory_human', 'Unknown'),
                'expired_items': redis_stats.get('expired_keys', 0)
            }
            
            # Format strategy cache counts properly  
            formatted_strategy_counts = {
                'active_items': sum(counts['active_items'] for counts in key_counts.values()),
                'expired_items': sum(counts['expired_items'] for counts in key_counts.values())
            }
            
            return {
                'current_config_version': current_version,
                'cached_last_run_timestamp': str(cached_timestamp) if cached_timestamp else None,
                'redis_stats': formatted_redis_stats,
                'strategy_cache_counts': formatted_strategy_counts,
                'cache_details': key_counts,
                'cache_efficiency': self._calculate_cache_efficiency(redis_stats)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {
                'current_config_version': None,
                'cached_last_run_timestamp': None,
                'redis_stats': {'active_items': 0, 'expired_items': 0},
                'strategy_cache_counts': {'active_items': 0, 'expired_items': 0},
                'cache_details': {},
                'cache_efficiency': 0.0
            }
    
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
