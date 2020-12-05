import json
from abc import ABC, abstractmethod
from xml.dom.minidom import parseString


class SaverABC(ABC):

    @abstractmethod
    def save(self, data, path, output_format) -> None:
        pass


class SaverJSON(SaverABC):

    def save(self, data, path, output_format):
        with open(f'{path}.{output_format}', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)


class SaverXML(SaverABC):

    def save(self, data, path, output_format) -> None:
        with open(f'{path}.{output_format}', 'w', encoding='utf-8') as f:
            f.write(str(data))
        







