from abc import ABC, abstractmethod

class Skill(ABC):
    """Abstract base class for all computational units."""
    def __init__(self, name: str, config: dict):
        self.name = name
        self.inputs = config.get('inputs',)
        self.outputs = config.get('outputs',)
        self.config = config.get('config', {})

    @abstractmethod
    def execute(self, data_context) -> None:
        """
        Executes the skill's logic.
        Reads from and writes to the data_context.
        """
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"