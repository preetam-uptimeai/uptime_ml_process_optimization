import yaml
from datetime import datetime
from typing import Optional
from .minio_client import get_minio_client

class ConfigManager:
    def __init__(self, timestamp_file: str = 'last_run_timestamp.yaml', 
                 deployed_config_file: str = 'metadata/deployed_config_version.yaml'):
        self.timestamp_file = timestamp_file
        self.deployed_config_file = deployed_config_file
        self.minio_client = get_minio_client()

    def get_last_run_timestamp(self) -> Optional[datetime]:
        """Read the last run timestamp from config file"""
        try:
            with open(self.timestamp_file, 'r') as f:
                config = yaml.safe_load(f)
                if config and 'last_run_timestamp' in config:
                    return datetime.fromisoformat(config['last_run_timestamp'])
        except Exception as e:
            print(f"Warning: Could not read last_run_timestamp from config: {e}")
        return None

    def update_last_run_timestamp(self, timestamp: datetime):
        """Write the last run timestamp to config file"""
        config = {'last_run_timestamp': timestamp.isoformat()}
        with open(self.timestamp_file, 'w') as f:
            yaml.dump(config, f)

    def get_deployed_config_version(self) -> str:
        """Read the deployed config version from metadata file"""
        try:
            with open(self.deployed_config_file, 'r') as f:
                config = yaml.safe_load(f)
                if config and 'config.yaml' in config:
                    return config['config.yaml']
                else:
                    raise Exception("config.yaml version not found in deployed_config_version.yaml")
        except Exception as e:
            raise Exception(f"Failed to read deployed config version: {str(e)}")

    def load_strategy_config_from_minio(self) -> dict:
        """Load strategy configuration from MinIO using deployed version"""
        try:
            # Get the deployed config version
            version = self.get_deployed_config_version()
            print(f"Loading config version {version} from MinIO...")
            
            # Load config from MinIO
            config = self.minio_client.get_config_by_version(version)
            print("Successfully loaded config from MinIO")
            return config
        except Exception as e:
            raise Exception(f"Failed to load strategy config from MinIO: {str(e)}")

    def load_strategy_config(self, config_file: str = 'config.yaml') -> dict:
        """Load strategy configuration from file (fallback method)"""
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Failed to load strategy config: {str(e)}")