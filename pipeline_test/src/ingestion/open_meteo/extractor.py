"""
OpenMEteo extractor
"""

from logging import Logger
import logging
from pathlib import Path
from datetime import datetime,date
from uuid import uuid4
from src.ingestion.comon.base_extractor import Base_extractor




logger = logging.getLogger(__name__)


class OpenMeteoExtractor:

    """extract data from the OpenMeteo api
    Uses the REST Describe dor to build the fields list
    """

    def __init__(self) -> None 