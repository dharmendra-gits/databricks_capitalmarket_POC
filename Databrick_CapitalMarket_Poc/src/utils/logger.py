"""
Logger Configuration for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement
"""

import logging
import os
from typing import Optional

class LoggerSetup:
    """Centralized logger setup for the application."""
    
    @staticmethod
    def setup_logger(
        name: str = 'capital_markets',
        level: str = 'INFO',
        log_format: Optional[str] = None,
        log_file: Optional[str] = None
    ) -> logging.Logger:
        """
        Setup and configure logger for the application.
        
        Args:
            name (str): Logger name
            level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_format (str): Custom log format string
            log_file (str): Log file path (optional)
            
        Returns:
            logging.Logger: Configured logger instance
        """
        # Create logger
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Set default format if not provided
        if log_format is None:
            log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
        formatter = logging.Formatter(log_format)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if log file specified)
        if log_file:
            # Create directory if it doesn't exist
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get existing logger by name.
        
        Args:
            name (str): Logger name
            
        Returns:
            logging.Logger: Logger instance
        """
        return logging.getLogger(name)
