import asyncio
import logging
import sys

# Local imports
from src.process_data import process_data
from config import logs_level

# Logging
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logs_level,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    process_data()
