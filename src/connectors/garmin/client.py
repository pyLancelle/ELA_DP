"""
Garmin Client Wrapper
---------------------
Handles authentication and session management for Garmin Connect.
"""
import os
import logging
from typing import Dict, Any

try:
    from garminconnect import Garmin
except ImportError:
    # This will be handled by the main entry point check
    pass

class GarminClient:
    """Wrapper for Garmin Connect client."""
    
    def __init__(self, env_vars: Dict[str, str]):
        self.username = env_vars.get("GARMIN_USERNAME")
        self.password = env_vars.get("GARMIN_PASSWORD")
        self.client = None
        
    def authenticate(self) -> Any:
        """Authenticate to Garmin Connect."""
        if not self.username or not self.password:
            raise ValueError("Missing Garmin credentials")
            
        try:
            self.client = Garmin(self.username, self.password)
            self.client.login()
            logging.info("✅ Authenticated to Garmin Connect")
            return self.client
        except Exception as e:
            raise ConnectionError(f"Authentication failed: {e}") from e
            
    def get_client(self) -> Any:
        """Get the authenticated client, connecting if necessary."""
        if not self.client:
            self.authenticate()
        return self.client
