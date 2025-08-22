"""
MinIO client utilities for reading configuration and model files from MinIO storage.
"""

import io
import json
import pickle
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from minio import Minio
from minio.error import S3Error
# Note: Import moved to avoid circular dependency


class MinIOClient:
    """MinIO client for reading configuration and model files."""
    
    def __init__(self, endpoint: str = "localhost:9090", 
                 access_key: str = "minioadmin", 
                 secret_key: str = "minioadmin123",
                 secure: bool = False):
        """
        Initialize MinIO client.
        
        Args:
            endpoint: MinIO server endpoint
            access_key: MinIO access key
            secret_key: MinIO secret key
            secure: Whether to use HTTPS
        """
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
    
    def _get_strategy_cache(self):
        """Lazy load strategy cache to avoid circular imports."""
        if self._strategy_cache is None:
            try:
                # Import here to avoid circular dependency
                import importlib
                strategy_cache_module = importlib.import_module('strategy-manager.strategy_cache')
                get_strategy_cache = strategy_cache_module.get_strategy_cache
                self._strategy_cache = get_strategy_cache()
            except ImportError as e:
                # If import fails, disable caching
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
            try:
                config_filename = f"configs/config-{version}.yaml"
                
                # Get object from MinIO
                response = self.client.get_object(self.config_bucket, config_filename)
                
                # Read and parse YAML
                config_data = yaml.safe_load(response.data.decode('utf-8'))
                response.close()
                response.release_conn()
                
                return config_data
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise FileNotFoundError(f"Config file {config_filename} not found in MinIO bucket {self.config_bucket}")
                raise Exception(f"MinIO error while reading config: {e}")
            except yaml.YAMLError as e:
                raise Exception(f"Error parsing YAML config: {e}")
            except Exception as e:
                raise Exception(f"Error reading config from MinIO: {e}")
        
        # Check cache first  
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            cached_config = strategy_cache.get_cached_config(version)
            if cached_config is not None:
                return cached_config
        
        # Load from MinIO and cache
        config = _load_config_from_minio(version)
        if strategy_cache:
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
            try:
                # Create temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pth')
                temp_path = temp_file.name
                temp_file.close()
                
                # Download from MinIO
                self.client.fget_object(self.models_bucket, model_path, temp_path)
                
                return temp_path
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise FileNotFoundError(f"Model file {model_path} not found in MinIO bucket {self.models_bucket}")
                raise Exception(f"MinIO error while downloading model: {e}")
            except Exception as e:
                raise Exception(f"Error downloading model from MinIO: {e}")
        
        # Check cache first
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            cached_model = strategy_cache.get_cached_model(model_path)
            if cached_model is not None:
                return cached_model
        
        # Download from MinIO and cache
        model = _download_model_from_minio(model_path)
        if strategy_cache:
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
            try:
                # Get object from MinIO
                response = self.client.get_object(self.models_bucket, scaler_path)
                
                # Deserialize pickle data
                scaler_data = pickle.loads(response.data)
                response.close()
                response.release_conn()
                
                return scaler_data
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise FileNotFoundError(f"Scaler file {scaler_path} not found in MinIO bucket {self.models_bucket}")
                raise Exception(f"MinIO error while reading scaler: {e}")
            except pickle.PickleError as e:
                raise Exception(f"Error deserializing pickle scaler: {e}")
            except Exception as e:
                raise Exception(f"Error reading scaler from MinIO: {e}")
        
        # Check cache first
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            cached_scaler = strategy_cache.get_cached_scaler(scaler_path)
            if cached_scaler is not None:
                return cached_scaler
        
        # Load from MinIO and cache
        scaler = _load_scaler_from_minio(scaler_path)
        if strategy_cache:
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
            try:
                # Get object from MinIO
                response = self.client.get_object(self.models_bucket, metadata_path)
                
                # Parse JSON
                metadata = json.loads(response.data.decode('utf-8'))
                response.close()
                response.release_conn()
                
                return metadata
                
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise FileNotFoundError(f"Metadata file {metadata_path} not found in MinIO bucket {self.models_bucket}")
                raise Exception(f"MinIO error while reading metadata: {e}")
            except json.JSONDecodeError as e:
                raise Exception(f"Error parsing JSON metadata: {e}")
            except Exception as e:
                raise Exception(f"Error reading metadata from MinIO: {e}")
        
        # Note: Metadata caching not implemented in new strategy cache
        # Load directly from MinIO for now
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
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f"Successfully uploaded {file_path} as {object_name} to {bucket_name}")
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
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
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix)
            return [obj.object_name for obj in objects]
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []
    
    def cleanup_temp_files(self, file_paths: list):
        """
        Clean up temporary files.
        
        Args:
            file_paths: List of temporary file paths to remove
        """
        for file_path in file_paths:
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception as e:
                print(f"Warning: Could not remove temporary file {file_path}: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            return strategy_cache.get_cache_stats()
        return {}
    
    def clear_all_caches(self) -> None:
        """Clear all MinIO caches."""
        strategy_cache = self._get_strategy_cache()
        if strategy_cache:
            strategy_cache.clear_all_caches()
    
    def cleanup_expired_temp_files(self) -> None:
        """Clean up expired temporary files from disk."""
        # Redis TTL handles cleanup automatically


def get_minio_client(configuration: Dict = None) -> MinIOClient:
    """
    Factory function to create MinIO client with configuration settings.
    
    Args:
        configuration: Configuration dictionary containing storage.minio settings
    
    Returns:
        Configured MinIOClient instance
    """
    if configuration and 'storage' in configuration and 'minio' in configuration['storage']:
        minio_config = configuration['storage']['minio']
        return MinIOClient(
            endpoint=minio_config.get('endpoint', 'localhost:9002'),
            access_key=minio_config.get('access_key', 'user'),
            secret_key=minio_config.get('secret_key', 'password'),
            secure=minio_config.get('secure', False)
        )
    
    # Fallback to default settings
    return MinIOClient(endpoint="localhost:9002", access_key="user", secret_key="password")
