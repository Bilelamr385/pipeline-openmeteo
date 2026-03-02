"""

abstract base class for data source extractors

"""

from abc import ABC , abstractmethod
from pathlib import Path
import datetime



class Base_extractor(ABC):

    @abstractmethod
    def extract_full(self)-> None :
        ...
        