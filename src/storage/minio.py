"""
MinIO client utilities for reading configuration and model files from MinIO storage.
"""

import io
import json
import pickle
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

import structlog
import yaml
from minio import Minio
from minio.error import S3Error
# Note: Import moved to avoid circular dependency

logger = structlog.get_logger(__name__)


class MinIOClient:
    """MinIO client for reading configuration and model files."""
    
    def __init__(self, endpoint: str = "localhost:9002", 
                 access_key: str = "user", 
                 secret_key: str = "password",
                 secure: bool = False):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: MinIO access key
            secret_key: MinIO secret key
            secure: Whether to use HTTPS
        """
        logger.info("Initializing MinIO client", 
                   endpoint=endpoint, 
                   secure=secure,
                   access_key_masked=f"{access_key[:4]}***" if len(access_key) > 4 else "***")
        
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            self.config_bucket = "process-optimization"
            self.models_bucket = "process-optimization"
            # Lazy import to avoid circular dependency
            self._strategy_cache = None
            
            logger.info("MinIO client initialized successfully", 
                       config_bucket=self.config_bucket,
                       models_bucket=self.models_bucket)
        except Exception as e:
            logger.error("Failed to initialize MinIO client", error=str(e), endpoint=endpoint)
            raise
    
    def _get_strategy_cache(self):
        """Lazy load strategy cache to avoid circular imports."""
        if self._strategy_cache is None:
            try:
                logger.debug("Loading strategy cache module")
                # Import here to avoid circular dependency
                import importlib
                strategy_cache_module = importlib.import_module('strategy-manager.strategy_cache')
                get_strategy_cache = strategy_cache_module.get_strategy_cache
                self._strategy_cache = get_strategy_cache()
                logger.debug("Strategy cache loaded successfully")
            except ImportError as e:
                # If import fails, disable caching
                logger.warning("Strategy cache not available, caching disabled", error=str(e))
                return None
        return self._strategy_cache
    
    def get_config_by_version(self, version: str) -> Dict[str, Any]:
        """
        Read configuration YAML file from MinIO by version with caching.
        
        Args:
            version: Version string (e.g., "1.0.0")
            
        Returns:
            Dictionary containing configuration data
            
        Raises:
            Exception: If config file not found or invalid
        """
        def _load_config_from_minio(version):
            config_filename = f"configs/config-{version}.yaml"
            logger.info("Loading config from MinIO", 
                       version=version, 
                       filename=config_filename, 
                       bucket=self.config_bucket)
            
            try:
                # Get object from MinIO
                response = self.client.get_object(self.config_bucket, config_filename)
                logger.debug("MinIO object retrieved successfully", filename=config_filename)
                
                # Read and parse YAML
                config_data = yaml.safe_load(response.data.decode('utf-8'))
                response.close()
                response.release_conn()
                
                logger.info("Config loaded and parsed successfully", 
                           version=version, 
                           config_keys=list(config_data.keys()) if isinstance(config_data, dict) else "non-dict")
                return config_data
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.error("Config file not found in MinIO", 
                                filename=config_filename, 
                                bucket=self.config_bucket,
                                version=version)
                    raise FileNotFoundError(f"Config file {config_filename} not found in MinIO bucket {self.config_bucket}")
                logger.error("MinIO S3 error while reading config", 
                            error=str(e), 
                            error_code=e.code,
                            filename=config_filename)
                raise Exception(f"MinIO error while reading config: {e}")
            except yaml.YAMLError as e:
                logger.error("YAML parsing error", 
                            error=str(e), 
                            filename=config_filename)
                raise Exception(f"Error parsing YAML config: {e}")
            except Exception as e:
                logger.error("Unexpected error reading config from MinIO", 
                            error=str(e), 
                            filename=config_filename)
                raise Exception(f"Error reading config from MinIO: {e}")
        
        # Check cache first  
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            logger.debug("Checking cache for config", version=version)
            cached_config = strategy_cache.get_cached_config(version)
            if cached_config is not None:
                logger.info("Config found in cache", version=version)
                return cached_config
            logger.debug("Config not found in cache", version=version)
        
        # Load from MinIO and cache
        config = _load_config_from_minio(version)
        if strategy_cache:
            logger.debug("Caching loaded config", version=version)
            strategy_cache.set_cached_config(version, config)
        return config
    
    def get_pytorch_model(self, model_path: str) -> str:
        """
        Download PyTorch model file (.pth) from MinIO to temporary location with caching.
        
        Args:
            model_path: Path to model file in MinIO (e.g., "saved_models/model.pth")
            
        Returns:
            Local path to downloaded model file
            
        Raises:
            Exception: If model file not found or download fails
        """
        def _download_model_from_minio(model_path):
            logger.info("Downloading model from MinIO", 
                       model_path=model_path, 
                       bucket=self.models_bucket)
            
            try:
                # Create temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pth')
                temp_path = temp_file.name
                temp_file.close()
                logger.debug("Created temporary file", temp_path=temp_path)
                
                # Download from MinIO
                self.client.fget_object(self.models_bucket, model_path, temp_path)
                logger.info("Model downloaded successfully", 
                           model_path=model_path, 
                           temp_path=temp_path,
                           file_size=Path(temp_path).stat().st_size)
                
                return temp_path
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.error("Model file not found in MinIO", 
                                model_path=model_path, 
                                bucket=self.models_bucket)
                    raise FileNotFoundError(f"Model file {model_path} not found in MinIO bucket {self.models_bucket}")
                logger.error("MinIO S3 error while downloading model", 
                            error=str(e), 
                            error_code=e.code,
                            model_path=model_path)
                raise Exception(f"MinIO error while downloading model: {e}")
            except Exception as e:
                logger.error("Unexpected error downloading model from MinIO", 
                            error=str(e), 
                            model_path=model_path)
                raise Exception(f"Error downloading model from MinIO: {e}")
        
        # Check cache first
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            logger.debug("Checking cache for model", model_path=model_path)
            cached_model = strategy_cache.get_cached_model(model_path)
            if cached_model is not None:
                # Validate that the cached temp file still exists
                if Path(cached_model).exists():
                    logger.info("Model found in cache and temp file exists", 
                               model_path=model_path, 
                               cached_file=cached_model)
                    return cached_model
                else:
                    logger.warning("Cached model temp file missing, invalidating cache", 
                                  model_path=model_path, 
                                  missing_file=cached_model)
                    # Invalidate this specific cache entry
                    strategy_cache.invalidate_cached_model(model_path)
            else:
                logger.debug("Model not found in cache", model_path=model_path)
        
        # Download from MinIO and cache
        model = _download_model_from_minio(model_path)
        if strategy_cache:
            logger.debug("Caching downloaded model", model_path=model_path)
            strategy_cache.set_cached_model(model_path, model)
        return model
    
    def get_pickle_scaler(self, scaler_path: str) -> Any:
        """
        Read pickle scaler file (.pkl) from MinIO and deserialize with caching.
        
        Args:
            scaler_path: Path to scaler file in MinIO (e.g., "saved_scalers/scaler.pkl")
            
        Returns:
            Deserialized scaler object
            
        Raises:
            Exception: If scaler file not found or deserialization fails
        """
        def _load_scaler_from_minio(scaler_path):
            logger.info("Loading scaler from MinIO", 
                       scaler_path=scaler_path, 
                       bucket=self.models_bucket)
            
            try:
                # Get object from MinIO
                response = self.client.get_object(self.models_bucket, scaler_path)
                logger.debug("MinIO object retrieved successfully", scaler_path=scaler_path)
                
                # Deserialize pickle data
                scaler_data = pickle.loads(response.data)
                response.close()
                response.release_conn()
                
                logger.info("Scaler loaded and deserialized successfully", 
                           scaler_path=scaler_path, 
                           scaler_type=type(scaler_data).__name__)
                return scaler_data
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.error("Scaler file not found in MinIO", 
                                scaler_path=scaler_path, 
                                bucket=self.models_bucket)
                    raise FileNotFoundError(f"Scaler file {scaler_path} not found in MinIO bucket {self.models_bucket}")
                logger.error("MinIO S3 error while reading scaler", 
                            error=str(e), 
                            error_code=e.code,
                            scaler_path=scaler_path)
                raise Exception(f"MinIO error while reading scaler: {e}")
            except pickle.PickleError as e:
                logger.error("Pickle deserialization error", 
                            error=str(e), 
                            scaler_path=scaler_path)
                raise Exception(f"Error deserializing pickle scaler: {e}")
            except Exception as e:
                logger.error("Unexpected error reading scaler from MinIO", 
                            error=str(e), 
                            scaler_path=scaler_path)
                raise Exception(f"Error reading scaler from MinIO: {e}")
        
        # Check cache first
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            logger.debug("Checking cache for scaler", scaler_path=scaler_path)
            cached_scaler = strategy_cache.get_cached_scaler(scaler_path)
            if cached_scaler is not None:
                try:
                    # Validate that the cached scaler is still usable
                    # Try to access a basic attribute to ensure it's not corrupted
                    _ = getattr(cached_scaler, '__class__', None)
                    logger.info("Scaler found in cache and validated", scaler_path=scaler_path)
                    return cached_scaler
                except Exception as e:
                    logger.warning("Cached scaler is corrupted, invalidating cache", 
                                  scaler_path=scaler_path, 
                                  error=str(e))
                    # Invalidate this specific cache entry
                    strategy_cache.invalidate_cached_scaler(scaler_path)
            else:
                logger.debug("Scaler not found in cache", scaler_path=scaler_path)
        
        # Load from MinIO and cache
        scaler = _load_scaler_from_minio(scaler_path)
        if strategy_cache:
            logger.debug("Caching loaded scaler", scaler_path=scaler_path)
            strategy_cache.set_cached_scaler(scaler_path, scaler)
        return scaler
    
    def get_json_metadata(self, metadata_path: str) -> Dict[str, Any]:
        """
        Read JSON metadata file from MinIO with caching.
        
        Args:
            metadata_path: Path to metadata file in MinIO (e.g., "saved_metadata/meta.json")
            
        Returns:
            Dictionary containing metadata
            
        Raises:
            Exception: If metadata file not found or invalid JSON
        """
        def _load_metadata_from_minio(metadata_path):
            logger.info("Loading metadata from MinIO", 
                       metadata_path=metadata_path, 
                       bucket=self.models_bucket)
            
            try:
                # Get object from MinIO
                response = self.client.get_object(self.models_bucket, metadata_path)
                logger.debug("MinIO object retrieved successfully", metadata_path=metadata_path)
                
                # Parse JSON
                metadata = json.loads(response.data.decode('utf-8'))
                response.close()
                response.release_conn()
                
                logger.info("Metadata loaded and parsed successfully", 
                           metadata_path=metadata_path, 
                           metadata_keys=list(metadata.keys()) if isinstance(metadata, dict) else "non-dict")
                return metadata
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.error("Metadata file not found in MinIO", 
                                metadata_path=metadata_path, 
                                bucket=self.models_bucket)
                    raise FileNotFoundError(f"Metadata file {metadata_path} not found in MinIO bucket {self.models_bucket}")
                logger.error("MinIO S3 error while reading metadata", 
                            error=str(e), 
                            error_code=e.code,
                            metadata_path=metadata_path)
                raise Exception(f"MinIO error while reading metadata: {e}")
            except json.JSONDecodeError as e:
                logger.error("JSON parsing error", 
                            error=str(e), 
                            metadata_path=metadata_path)
                raise Exception(f"Error parsing JSON metadata: {e}")
            except Exception as e:
                logger.error("Unexpected error reading metadata from MinIO", 
                            error=str(e), 
                            metadata_path=metadata_path)
                raise Exception(f"Error reading metadata from MinIO: {e}")
        
        # Note: Metadata caching not implemented in new strategy cache
        # Load directly from MinIO for now
        logger.debug("Loading metadata directly from MinIO (no caching)", metadata_path=metadata_path)
        return _load_metadata_from_minio(metadata_path)
    
    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        """
        Upload a file to MinIO bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Object name in the bucket
            file_path: Local path to file to upload
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("Uploading file to MinIO", 
                   file_path=file_path, 
                   object_name=object_name, 
                   bucket=bucket_name,
                   file_size=Path(file_path).stat().st_size if Path(file_path).exists() else "unknown")
        
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            logger.info("File uploaded successfully", 
                       file_path=file_path, 
                       object_name=object_name, 
                       bucket=bucket_name)
            return True
        except Exception as e:
            logger.error("Error uploading file", 
                        error=str(e), 
                        file_path=file_path, 
                        object_name=object_name, 
                        bucket=bucket_name)
            return False
    
    def list_objects(self, bucket_name: str, prefix: str = "") -> list:
        """
        List objects in a MinIO bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Prefix to filter objects
            
        Returns:
            List of object names
        """
        logger.debug("Listing objects in MinIO bucket", 
                    bucket=bucket_name, 
                    prefix=prefix)
        
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix)
            object_list = [obj.object_name for obj in objects]
            logger.info("Objects listed successfully", 
                       bucket=bucket_name, 
                       prefix=prefix, 
                       object_count=len(object_list))
            return object_list
        except Exception as e:
            logger.error("Error listing objects", 
                        error=str(e), 
                        bucket=bucket_name, 
                        prefix=prefix)
            return []
    
    def cleanup_temp_files(self, file_paths: list, force_cleanup: bool = False):
        """
        Clean up temporary files, with smart caching awareness.
        
        Args:
            file_paths: List of temporary file paths to remove
            force_cleanup: If True, cleanup regardless of cache status
        """
        logger.info("Cleaning up temporary files", 
                   file_count=len(file_paths), 
                   force_cleanup=force_cleanup)
        
        for file_path in file_paths:
            try:
                should_cleanup = force_cleanup
                
                if not should_cleanup:
                    # Check if the file is still needed by checking cache
                    # This is a safety check for version-based persistence
                    strategy_cache = self._get_strategy_cache()
                    if strategy_cache:
                        # For now, be conservative and only cleanup on force
                        # In minute-based cycles, we want to preserve files
                        should_cleanup = False
                        logger.debug("Skipping temp file cleanup due to frequent cycles", 
                                   file_path=file_path)
                    else:
                        should_cleanup = True
                
                if should_cleanup and Path(file_path).exists():
                    Path(file_path).unlink(missing_ok=True)
                    logger.debug("Temporary file removed", file_path=file_path)
                elif not Path(file_path).exists():
                    logger.debug("Temporary file already removed", file_path=file_path)
                else:
                    logger.debug("Temporary file preserved for cache efficiency", file_path=file_path)
                    
            except Exception as e:
                logger.warning("Could not remove temporary file", 
                              file_path=file_path, 
                              error=str(e))
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        logger.debug("Retrieving cache statistics")
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            stats = strategy_cache.get_cache_stats()
            logger.debug("Cache statistics retrieved", stats_keys=list(stats.keys()) if stats else "empty")
            return stats
        logger.debug("No strategy cache available, returning empty stats")
        return {}
    
    def clear_all_caches(self) -> None:
        """Clear all MinIO caches."""
        logger.info("Clearing all MinIO caches")
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            strategy_cache.clear_all_caches()
            logger.info("All MinIO caches cleared successfully")
        else:
            logger.warning("No strategy cache available to clear")
    
    def cleanup_on_version_change(self) -> None:
        """
        Force cleanup of all temporary files when strategy version changes.
        This should be called when strategy_version.yaml changes.
        """
        logger.info("Performing cleanup due to strategy version change")
        
        # This would typically be called by the strategy manager
        # when version change is detected. For now, we set up the framework.
        import tempfile
        import os
        
        # Get temp directory and look for our temp files
        temp_dir = Path(tempfile.gettempdir())
        
        # Find temp files that might be ours (with .pth extension)
        temp_files = []
        for temp_file in temp_dir.glob("tmp*"):
            if temp_file.suffix in ['.pth', '.pkl'] and temp_file.is_file():
                try:
                    # Check if file is old enough to be from previous versions
                    file_age = time.time() - temp_file.stat().st_mtime
                    if file_age > 300:  # Older than 5 minutes
                        temp_files.append(str(temp_file))
                except:
                    pass
        
        if temp_files:
            # Force cleanup of old temp files
            self.cleanup_temp_files(temp_files, force_cleanup=True)
            logger.info(f"Cleaned up {len(temp_files)} old temporary files due to version change")
    
    def cleanup_expired_temp_files(self) -> None:
        """Clean up expired temporary files from disk."""
        # Redis TTL handles cleanup automatically
        logger.debug("Cleanup of expired temp files is handled automatically by Redis TTL")


def get_minio_client(configuration: Dict = None) -> MinIOClient:
    """
    Factory function to create MinIO client with configuration settings.
    
    Args:
        configuration: Configuration dictionary containing storage.minio settings
    
    Returns:
        Configured MinIOClient instance
    """
    logger.debug("Creating MinIO client from configuration")
    
    if configuration and 'storage' in configuration and 'minio' in configuration['storage']:
        minio_config = configuration['storage']['minio']
        logger.debug("Using MinIO configuration from config file", 
                    endpoint=minio_config.get('endpoint', 'localhost:9002'))
        return MinIOClient(
            endpoint=minio_config.get('endpoint', 'localhost:9002'),
            access_key=minio_config.get('access_key', 'user'),
            secret_key=minio_config.get('secret_key', 'password'),
            secure=minio_config.get('secure', False)
        )
    
    # Fallback to default settings
    logger.debug("Using default MinIO configuration (fallback)")
    return MinIOClient(endpoint="localhost:9002", access_key="user", secret_key="password")
