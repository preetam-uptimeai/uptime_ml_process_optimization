"""Task interface definitions."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class TaskInterface(ABC):
    """Abstract base class for optimization tasks."""
    
    @abstractmethod
    def execute(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the optimization task."""
        pass
    
    @abstractmethod
    def validate_parameters(self, parameters: Dict[str, Any]) -> bool:
        """Validate task parameters."""
        pass
    
    @abstractmethod
    def get_task_info(self) -> Dict[str, Any]:
        """Get task information and metadata."""
        pass
