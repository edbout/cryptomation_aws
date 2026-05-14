import logging
import requests
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)  

class GeoBlockChecker:
    """
    Check Polymarket geoblocking status for the current IP/location.
    """
    
    GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
    TIMEOUT = 10
    
    def __init__(self):
        self._cache_ttl = 300  # 5 minutes default
        self._last_check = 0
        self._cached_result = None
    
    def check_geoblock(self) -> Dict[str, Any]:
        """
        Call Polymarket's geoblock endpoint to check if trading is allowed.
        """
        try:
            response = requests.get(self.GEOBLOCK_URL, timeout=self.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"❗ check_geoblock | Geoblock check failed: {e}")
            return {"blocked": True, "ip": "unknown", "country": "UNKNOWN", "region": ""}
    
    def test_geo(self) -> bool:
        """
        Check if trading is allowed and log the result.
        """
        geo = self.check_geoblock()
        
        if geo.get("blocked", True):
            country = geo.get("country", "UNKNOWN")
            region = geo.get("region", "")
            if region:
                logger.warning(f"✗ test_geo | Trading not available in {country} ({region})")
            else:
                logger.warning(f"✗ test_geo | Trading not available in {country}")
            return False
        else:
            logger.info("✓ test_geo | Trading available from this IP")
            return True
    
    def test_geo_cached(self) -> bool:
        """Test geo with 5-minute caching."""
        now = time.time()
        if (self._cached_result is not None and 
            now - self._last_check < self._cache_ttl):
            return self._cached_result
        
        self._cached_result = self.test_geo()
        self._last_check = now
        return self._cached_result
