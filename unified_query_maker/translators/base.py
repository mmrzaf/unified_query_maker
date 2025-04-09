from abc import ABC, abstractmethod
from typing import Dict, Union


class QueryTranslator(ABC):
    @abstractmethod
    def translate(self, query: Dict) -> Union[str, Dict]:
        pass
