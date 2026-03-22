"""
Configuration Loader for Databricks Medallion Architecture
Capital Markets Post-Trade Settlement
"""

import yaml
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Loads and manages application configuration from YAML files."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path (str): Path to configuration YAML file
        """
        if config_path is None:
            # Default to config/config.yaml in project root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(project_root, 'config', 'config.yaml')
        
        self.config_path = config_path
        self.config = None
        
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Loaded configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is malformed
        """
        try:
            with open(self.config_path, 'r') as file:
                self.config = yaml.safe_load(file)
                logger.info(f"Configuration loaded successfully from {self.config_path}")
                return self.config
                
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading configuration: {e}")
            raise
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get loaded configuration. Loads if not already loaded.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        if self.config is None:
            return self.load_config()
        return self.config
    
    def get_section(self, section_name: str) -> Dict[str, Any]:
        """
        Get specific configuration section.
        
        Args:
            section_name (str): Name of configuration section
            
        Returns:
            Dict[str, Any]: Configuration section
            
        Raises:
            KeyError: If section doesn't exist
        """
        config = self.get_config()
        if section_name not in config:
            logger.error(f"Configuration section '{section_name}' not found")
            raise KeyError(f"Section '{section_name}' not found in configuration")
        
        return config[section_name]
    
    def get_value(self, key_path: str, default=None):
        """
        Get configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to configuration value
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        config = self.get_config()
        keys = key_path.split('.')
        
        current = config
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            if default is not None:
                logger.warning(f"Configuration key '{key_path}' not found, using default value")
                return default
            else:
                logger.error(f"Configuration key '{key_path}' not found")
                raise KeyError(f"Key '{key_path}' not found in configuration")
