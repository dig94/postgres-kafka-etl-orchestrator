from abc import ABC, abstractmethod
from typing import List, Tuple, Any

class BaseConnector(ABC):
    """
    Standardized interface for all connectors.
    """

    @abstractmethod
    def list_entities(self) -> List[str]:
        """List available entities (tables, collections, buckets, etc.)"""
        pass

    
    def get_bounds(self, entity: str, key_column: str) -> Tuple[Any, Any]:
        raise NotImplementedError("Bounds not supported for this connector")
        """Get partition bounds for scalable extraction."""
        

    
    def read_range(self, entity: str, key_column: str, start: Any, end: Any):
        raise NotImplementedError("Range reads not supported for this connector")
        """Read one range or batch of data."""
        

    @abstractmethod
    def extract_data(self, entity: str, key_column: str, partitions: int = 8):
        """Top-level unified extraction entrypoint."""
        pass
