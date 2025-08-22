"""Storage interface definitions."""

from abc import ABC, abstractmethod
from typing import Any, Optional


class StorageInterface(ABC):
    """Abstract base class for storage implementations."""
    
    @abstractmethod
    def upload(self, bucket: str, object_name: str, data: Any) -> bool:
        """Upload data to storage."""
        pass
    
    @abstractmethod
    def download(self, bucket: str, object_name: str) -> Optional[Any]:
        """Download data from storage."""
        pass
    
    @abstractmethod
    def exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists in storage."""
        pass
    
    @abstractmethod
    def delete(self, bucket: str, object_name: str) -> bool:
        """Delete object from storage."""
        pass
