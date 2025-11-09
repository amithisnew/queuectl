"""
Utility functions for QueueCTL.
"""

import logging
import sys


def setup_logging(level: str = 'INFO'):
    """
    Setup logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )
    
    # Reduce noise from some libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)