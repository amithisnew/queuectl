"""
Configuration management for QueueCTL.
"""

from typing import Optional, Dict


DEFAULT_CONFIG = {
    'max_retries': '3',
    'backoff_base': '2',
    'worker_default_count': '1',
    'abandoned_threshold': '3600',
    'poll_interval': '1.0',
    'log_level': 'INFO'
}


class Config:
    """Configuration manager."""
    
    def __init__(self, storage):
        self.storage = storage
        self._ensure_defaults()
    
    def _ensure_defaults(self):
        """Ensure default configuration values exist."""
        existing = self.storage.get_all_config()
        for key, value in DEFAULT_CONFIG.items():
            if key not in existing:
                self.storage.set_config(key, value)
    
    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value."""
        value = self.storage.get_config(key)
        if value is None:
            value = DEFAULT_CONFIG.get(key, default)
        return value
    
    def get_int(self, key: str, default: int) -> int:
        """Get configuration value as integer."""
        value = self.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default
    
    def get_float(self, key: str, default: float) -> float:
        """Get configuration value as float."""
        value = self.get(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default
    
    def set(self, key: str, value: str):
        """Set configuration value."""
        self.storage.set_config(key, str(value))
    
    def get_all(self) -> Dict[str, str]:
        """Get all configuration values."""
        return self.storage.get_all_config()