import json
from abc import ABC, abstractmethod


class LoaderABC(ABC):

    @abstractmethod
    def load(self, path):
        pass


class FileLoader(LoaderABC):

    def load(self, path: str):
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise Exception(f'File not found {path}')



