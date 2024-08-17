from abc import ABC, abstractmethod


class QueryTranslator(ABC):
    @abstractmethod
    def translate(self, query):
        pass
