from abc import ABC, abstractmethod
from typing import Dict, Any
from unified_query_maker.models import QueryOutput


class QueryTranslator(ABC):
    """
    Abstract Base Class for all UQL query translators.
    """

    @abstractmethod
    def translate(self, query: Dict[str, Any]) -> QueryOutput:
        """
        Translates a UQL query dictionary into a database-specific query.

        Args:
            query: The raw UQL query dictionary.

        Returns:
            A database-specific query (e.g., a SQL string or an ES dict).
        """
        pass
