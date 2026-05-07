import os
import redis
import logging
import time

logger = logging.getLogger(__name__)

# Load .env safely
if os.getenv("HEROKU") != "true":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass

class Config:
    """Centralized configuration from environment variables (Heroku config vars)."""
    
    # Metamask Keys (required)
    PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
    PUBLIC_KEY = os.getenv("PUBLIC_KEY", "").strip() 
    PROXY_WALLET = os.getenv("PROXY_WALLET", "").strip()
    
    # Polymarket API builder code
    BUILDER_CODE = os.getenv("BUILDER_CODE", "").strip()

    # Runtime mode
    DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
    
    RPC = os.getenv("RPC", "https://polygon-mainnet.g.alchemy.com/v2/t4rRXOnpdSGvA2BRO1awx")

    # Trading sizes and thresholds (with validation)
    POSITION_SIZE = float(os.getenv("POSITION_SIZE", "5.0"))
    PRICE_MIN = float(os.getenv("PRICE_MIN", "0.10"))
    PRICE_MAX = float(os.getenv("PRICE_MAX", "0.90"))
    EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "10.0")) 
    MIN_WIN_RATE_THRESHOLD = float(os.getenv("MIN_WIN_RATE_THRESHOLD", "60.0"))
    BAR_OPEN_MIN_PCT = 0.10 # bybit 5m move must be at least 0.10%
    BAR_OPEN_EDGE_SURCHARGE = 3.0  # extra edge % required on top of dynamic calc
    HIST_PARAMS_DEFAULT = {"time_window": 15, "pct_tol": 0.025, "min_matches": 10}

    # Epoch bias & lead-lag thresholds
    EPOCH_BIAS_SECS = 30          # seconds into 5m epoch where market makers lag
    REDUCED_THRESHOLD_PCT = 0.03  # relaxed pct_change gate during bias/lag windows
    BTC_LAG_TTL = 60.0            # seconds BTC momentum flag stays active for other assets

    # Polymarket CLOB pre-trade liquidity checks
    CLOB_MAX_SPREAD = float(os.getenv("CLOB_MAX_SPREAD", "0.05"))          # max bid-ask spread (e.g. 0.05 = 5 cents)
    CLOB_MAX_SLIPPAGE_PCT = float(os.getenv("CLOB_MAX_SLIPPAGE_PCT", "2.0"))  # max estimated slippage %

    # Kelly Criterion position sizing
    # f* = (b*p - q) / b  where b = (1 - price) / price, p = win_rate, q = 1 - p
    KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))   # fractional Kelly (0.25 = quarter Kelly)
    KELLY_BANKROLL = float(os.getenv("KELLY_BANKROLL", "50.0"))   # trading capital in USD to size against
    KELLY_MIN_BET  = float(os.getenv("KELLY_MIN_BET",  "1.0"))    # floor (Polymarket CLOB minimum)
    KELLY_MAX_BET  = float(os.getenv("KELLY_MAX_BET",  "10.0"))   # ceiling (safety cap)

    # Volume filter (your new addition - perfect!)
    REQUIRE_VOL = os.getenv("REQUIRE_VOL", "true").lower() == "true"  # More explicit
    
    # Assets and symbols (can be expanded easily)
    ASSETS = ["BTCUSDT","ETHUSDT","XRPUSDT","SOLUSDT","DOGEUSDT"]
    
    BYBIT_SYMBOLS = ["BTCUSD", "ETHUSD", "XRPUSD", "SOLUSD", "DOGEUSD"]
    
    COINBASE_SYMBOLS = {
        "BTC-PERP": "BTC-PERP-INTX",
        "ETH-PERP": "ETH-PERP-INTX",
        "XRP-PERP": "XRP-PERP-INTX",
        "SOL-PERP": "SOL-PERP-INTX",
        "DOGE-PERP": "DOGE-PERP-INTX"   
    }
    
    CHAINLINK_SYMBOLS = {
        "btc/usd": 0.0,
        "eth/usd": 0.0,
        "xrp/usd": 0.0,
        "sol/usd": 0.0,
        "doge/usd": 0.0,
    }
    WS_URL = "wss://ws-live-data.polymarket.com"  
    CHAINLINK_FEED = "crypto_prices_chainlink"

    @classmethod
    def validate(cls):
        """Validate required config."""
        if not all([cls.PRIVATE_KEY, cls.PUBLIC_KEY, cls.PROXY_WALLET]):
            raise ValueError("Missing required keys: PRIVATE_KEY, PUBLIC_KEY, PROXY_WALLET")
        if cls.PRICE_MIN >= cls.PRICE_MAX:
            raise ValueError("PRICE_MIN must be < PRICE_MAX")
        
        # New validation: ASSETS count must match BYBIT_SYMBOLS
        if len(cls.ASSETS) != len(cls.BYBIT_SYMBOLS):
            raise ValueError(
                f"ASSETS count ({len(cls.ASSETS)}) must match BYBIT_SYMBOLS count ({len(cls.BYBIT_SYMBOLS)}). "
                f"ASSETS: {cls.ASSETS}, BYBIT_SYMBOLS: {cls.BYBIT_SYMBOLS}"
            )
                
class RedisCache:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self.client = self._connect()
        self._initialized = True
    
    def _connect(self):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                client = redis.from_url(
                    os.environ.get("REDISCLOUD_URL", "redis://localhost:6379"),
                    decode_responses=True,
                    socket_connect_timeout=10,
                    socket_timeout=10,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                client.ping()
                logger.info("✓ connect | to Redis successfully")
                return client
            except Exception as e:
                wait = 2 ** attempt
                logger.warning(f"✗ connect | to Redis attempt {attempt+1}/{max_retries} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
        logger.error("✗ connect | Redis permanently unavailable")
        return None
    
    @property
    def is_connected(self):
        """Check if client is connected."""
        return self.client is not None
    
    def ping(self):
        """Explicit ping for connection check; reconnects if dead."""
        if not self.client:
            logger.warning("✗ ping | Redis client is None, reconnecting...")
            self.client = self._connect()
        if not self.client:
            return False
        try:
            if self.client.ping():
                return True
        except Exception as e:
            logger.error(f"✗ ping | failed: {e}")
            self.client = None
        return False

    def __getattr__(self, name):
        if self.client:
            return getattr(self.client, name)
        raise AttributeError(f"✗ getattr | Redis client unavailable; cannot call: {name}")
    