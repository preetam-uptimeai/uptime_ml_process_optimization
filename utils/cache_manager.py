"""
Memory cache manager for models, scalers, configs and other MinIO artifacts.
Provides in-memory caching to avoid redundant downloads from MinIO.
"""

import threading
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
import weakref
import torch
import pickle
import json
import yaml
from pathlib import Path


class MemoryCache:
    """Thread-safe in-memory cache for MinIO artifacts with TTL support."""
    
    def __init__(self, default_ttl_hours: int = 24):
        """
        Initialize the cache with optional TTL (Time To Live).
        
        Args:
            default_ttl_hours: Default cache expiration time in hours
        """
        self._cache = {}
        self._timestamps = {}
        self._lock = threading.RLock()
        self.default_ttl = timedelta(hours=default_ttl_hours)
        
    def get(self, key: str) -> Optional[Any]:
        """
        Get item from cache if it exists and hasn't expired.
        
        Args:
            key: Cache key
            
        Returns:
            Cached item or None if not found/expired
        """
        with self._lock:
            if key not in self._cache:
                return None
                
            # Check TTL
            timestamp = self._timestamps.get(key)
            if timestamp and datetime.now() - timestamp > self.default_ttl:
                # Expired, remove from cache
                del self._cache[key]
                del self._timestamps[key]
                return None
                
            return self._cache[key]
    
    def set(self, key: str, value: Any) -> None:
        """
        Set item in cache with current timestamp.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = datetime.now()
    
    def delete(self, key: str) -> None:
        """
        Remove item from cache.
        
        Args:
            key: Cache key to remove
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            if key in self._timestamps:
                del self._timestamps[key]
    
    def clear(self) -> None:
        """Clear all items from cache."""
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            total_items = len(self._cache)
            expired_items = 0
            now = datetime.now()
            
            for timestamp in self._timestamps.values():
                if timestamp and now - timestamp > self.default_ttl:
                    expired_items += 1
                    
            return {
                'total_items': total_items,
                'expired_items': expired_items,
                'active_items': total_items - expired_items,
                'cache_keys': list(self._cache.keys())
            }


class CacheManager:
    """Singleton cache manager for MinIO artifacts with version-aware invalidation."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not getattr(self, '_initialized', False):
            # Setup logger
            self.logger = logging.getLogger("process_optimization.cache_manager")
            
            self.model_cache = MemoryCache(default_ttl_hours=24)
            self.scaler_cache = MemoryCache(default_ttl_hours=24)
            self.metadata_cache = MemoryCache(default_ttl_hours=24)
            self.config_cache = MemoryCache(default_ttl_hours=6)  # Shorter TTL for configs
            self.temp_files_cache = MemoryCache(default_ttl_hours=24)
            
            # Version tracking for cache invalidation
            self._current_config_version = None
            self._version_lock = threading.RLock()
            
            # Timestamp caching
            self._cached_last_run_timestamp = None
            self._timestamp_lock = threading.RLock()
            
            self._initialized = True
    
    def get_pytorch_model(self, model_path: str, loader_func) -> torch.nn.Module:
        """
        Get PyTorch model from cache or load via provided function.
        
        Args:
            model_path: Path to model in MinIO
            loader_func: Function to load model if not in cache
            
        Returns:
            PyTorch model
        """
        cache_key = f"pytorch_model:{model_path}"
        cached_model = self.model_cache.get(cache_key)
        
        if cached_model is not None:
            self.logger.debug(f"Using cached PyTorch model: {model_path}")
            return cached_model
        
        self.logger.info(f"Loading PyTorch model from MinIO: {model_path}")
        model = loader_func(model_path)
        
        if model is not None:
            self.model_cache.set(cache_key, model)
            self.logger.debug(f"Cached PyTorch model: {model_path}")
        
        return model
    
    def get_temp_model_path(self, model_path: str, loader_func) -> str:
        """
        Get temporary model file path from cache or download via provided function.
        
        Args:
            model_path: Path to model in MinIO
            loader_func: Function to download model if not in cache
            
        Returns:
            Local temporary file path
        """
        cache_key = f"temp_model_path:{model_path}"
        cached_path = self.temp_files_cache.get(cache_key)
        
        if cached_path is not None and Path(cached_path).exists():
            self.logger.debug(f"Using cached model file: {model_path}")
            return cached_path
        
        self.logger.info(f"Downloading model from MinIO: {model_path}")
        temp_path = loader_func(model_path)
        
        if temp_path is not None:
            self.temp_files_cache.set(cache_key, temp_path)
            self.logger.debug(f"Cached model file: {model_path}")
        
        return temp_path
    
    def get_pickle_scaler(self, scaler_path: str, loader_func) -> Any:
        """
        Get pickle scaler from cache or load via provided function.
        
        Args:
            scaler_path: Path to scaler in MinIO
            loader_func: Function to load scaler if not in cache
            
        Returns:
            Deserialized scaler object
        """
        cache_key = f"pickle_scaler:{scaler_path}"
        cached_scaler = self.scaler_cache.get(cache_key)
        
        if cached_scaler is not None:
            self.logger.debug(f"Using cached scaler: {scaler_path}")
            return cached_scaler
        
        self.logger.info(f"Loading scaler from MinIO: {scaler_path}")
        scaler = loader_func(scaler_path)
        
        if scaler is not None:
            self.scaler_cache.set(cache_key, scaler)
            self.logger.debug(f"Cached scaler: {scaler_path}")
        
        return scaler
    
    def get_json_metadata(self, metadata_path: str, loader_func) -> Dict[str, Any]:
        """
        Get JSON metadata from cache or load via provided function.
        
        Args:
            metadata_path: Path to metadata in MinIO
            loader_func: Function to load metadata if not in cache
            
        Returns:
            Metadata dictionary
        """
        cache_key = f"json_metadata:{metadata_path}"
        cached_metadata = self.metadata_cache.get(cache_key)
        
        if cached_metadata is not None:
            self.logger.debug(f"Using cached metadata: {metadata_path}")
            return cached_metadata
        
        self.logger.info(f"Loading metadata from MinIO: {metadata_path}")
        metadata = loader_func(metadata_path)
        
        if metadata is not None:
            self.metadata_cache.set(cache_key, metadata)
            self.logger.debug(f"Cached metadata: {metadata_path}")
        
        return metadata
    
    def check_and_invalidate_on_version_change(self, new_version: str) -> bool:
        """
        Check if config version has changed and invalidate all caches if needed.
        
        Args:
            new_version: New config version to check against
            
        Returns:
            True if cache was invalidated due to version change, False otherwise
        """
        with self._version_lock:
            if self._current_config_version is None:
                # First time setting version
                self._current_config_version = new_version
                self.logger.info(f"Initial config version set: {new_version}")
                return False
            
            if self._current_config_version != new_version:
                # Version changed - invalidate all caches
                self.logger.warning(f"Config version changed: {self._current_config_version} -> {new_version}")
                self.logger.info("Invalidating all caches due to version change...")
                
                self._clear_all_caches_internal()
                self._current_config_version = new_version
                
                self.logger.info(f"Cache invalidated and updated to version: {new_version}")
                return True
            
            return False
    
    def get_current_config_version(self) -> Optional[str]:
        """Get the currently cached config version."""
        with self._version_lock:
            return self._current_config_version
    
    def get_cached_last_run_timestamp(self) -> Optional[datetime]:
        """Get the cached last run timestamp."""
        with self._timestamp_lock:
            return self._cached_last_run_timestamp
    
    def set_cached_last_run_timestamp(self, timestamp: datetime) -> None:
        """Set the cached last run timestamp."""
        with self._timestamp_lock:
            self._cached_last_run_timestamp = timestamp
            self.logger.debug(f"Cached last run timestamp: {timestamp}")
    
    def get_last_run_timestamp_with_cache(self, file_loader_func) -> Optional[datetime]:
        """
        Get last run timestamp from cache or load from file.
        
        Args:
            file_loader_func: Function to load timestamp from file if not cached
            
        Returns:
            Last run timestamp or None if not available
        """
        with self._timestamp_lock:
            # Check if we have cached timestamp
            if self._cached_last_run_timestamp is not None:
                self.logger.debug(f"Using cached last run timestamp: {self._cached_last_run_timestamp}")
                return self._cached_last_run_timestamp
            
            # Load from file and cache it
            self.logger.info("Loading last run timestamp from file...")
            timestamp = file_loader_func()
            
            if timestamp is not None:
                self._cached_last_run_timestamp = timestamp
                self.logger.debug(f"Cached last run timestamp from file: {timestamp}")
            else:
                self.logger.warning("No last run timestamp found in file")
            
            return timestamp
    
    def _clear_all_caches_internal(self) -> None:
        """Internal method to clear all caches without version checking."""
        self.model_cache.clear()
        self.scaler_cache.clear()
        self.metadata_cache.clear()
        self.config_cache.clear()
        self.temp_files_cache.clear()
        
        # Clear timestamp cache as well
        with self._timestamp_lock:
            self._cached_last_run_timestamp = None
        
        # Note: Temporary files cleanup will be handled separately
    
    def get_config_by_version(self, version: str, loader_func) -> Dict[str, Any]:
        """
        Get config by version from cache or load via provided function.
        Automatically invalidates cache if version changes.
        
        Args:
            version: Config version
            loader_func: Function to load config if not in cache
            
        Returns:
            Configuration dictionary
        """
        # Check for version change and invalidate if needed
        version_changed = self.check_and_invalidate_on_version_change(version)
        
        cache_key = f"config_version:{version}"
        cached_config = self.config_cache.get(cache_key)
        
        if cached_config is not None and not version_changed:
            self.logger.debug(f"Using cached config version: {version}")
            return cached_config
        
        if version_changed:
            self.logger.info(f"Loading fresh config after version change: {version}")
        else:
            self.logger.info(f"Loading config from MinIO: version {version}")
        
        config = loader_func(version)
        
        if config is not None:
            self.config_cache.set(cache_key, config)
            self.logger.debug(f"Cached config version: {version}")
        
        return config
    
    def clear_all_caches(self) -> None:
        """Clear all caches and reset version tracking."""
        with self._version_lock:
            self._clear_all_caches_internal()
            self._current_config_version = None
            self.logger.info("Cleared all caches, reset version tracking, and cleared timestamp cache")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics including version and timestamp info.
        
        Returns:
            Dictionary with all cache statistics
        """
        with self._version_lock, self._timestamp_lock:
            return {
                'current_config_version': self._current_config_version,
                'cached_last_run_timestamp': self._cached_last_run_timestamp,
                'model_cache': self.model_cache.get_stats(),
                'scaler_cache': self.scaler_cache.get_stats(),
                'metadata_cache': self.metadata_cache.get_stats(),
                'config_cache': self.config_cache.get_stats(),
                'temp_files_cache': self.temp_files_cache.get_stats()
            }
    
    def cleanup_expired_temp_files(self) -> None:
        """Clean up expired temporary files from disk."""
        with self.temp_files_cache._lock:
            expired_keys = []
            now = datetime.now()
            
            for key, timestamp in self.temp_files_cache._timestamps.items():
                if timestamp and now - timestamp > self.temp_files_cache.default_ttl:
                    file_path = self.temp_files_cache._cache.get(key)
                    if file_path and Path(file_path).exists():
                        try:
                            Path(file_path).unlink()
                            self.logger.debug(f"Cleaned up expired temp file: {file_path}")
                        except Exception as e:
                            self.logger.warning(f"Failed to cleanup temp file {file_path}: {e}")
                    expired_keys.append(key)
            
            # Remove expired entries from cache
            for key in expired_keys:
                self.temp_files_cache.delete(key)


def get_cache_manager() -> CacheManager:
    """
    Factory function to get the singleton cache manager.
    
    Returns:
        CacheManager instance
    """
    return CacheManager()
