import os
import redis
import logging
import time

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
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
    # Markets with mid >= this threshold are treated as near-resolved and skipped early,
    # before order construction and validate_adjust_price are invoked.
    # Must be >= PRICE_MAX to avoid masking valid high-confidence entries.
    NEAR_RESOLVED_THRESHOLD = float(os.getenv("NEAR_RESOLVED_THRESHOLD", "0.95"))
    EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "10.0")) 
    MIN_WIN_RATE_THRESHOLD = float(os.getenv("MIN_WIN_RATE_THRESHOLD", "60.0"))
    BAR_OPEN_MIN_PCT = 0.03 # bybit 5m move must be at least 0.03%
    BAR_OPEN_EDGE_SURCHARGE = 3.0  # extra edge % required on top of dynamic calc
    HIST_PARAMS_DEFAULT = {"time_window": 15, "pct_tol": 0.025, "min_matches": 10}

    # Epoch bias & lead-lag thresholds
    EPOCH_BIAS_SECS = 30          # seconds into 5m epoch where market makers lag
    REDUCED_THRESHOLD_PCT = 0.03  # relaxed pct_change gate during bias/lag windows
    BTC_LAG_TTL = 60.0            # seconds BTC momentum flag stays active for other assets

    # Polymarket CLOB pre-trade liquidity checks
    CLOB_MAX_SPREAD = float(os.getenv("CLOB_MAX_SPREAD", "0.30"))          # max bid-ask spread (e.g. 0.30 = 30 cents)
    CLOB_MAX_SLIPPAGE_PCT = float(os.getenv("CLOB_MAX_SLIPPAGE_PCT", "7.0"))  # max estimated slippage %

    # Kelly Criterion position sizing
    # f* = (b*p - q) / b  where b = (1 - price) / price, p = win_rate, q = 1 - p
    KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.25"))   # fractional Kelly (0.25 = quarter Kelly)
    KELLY_BANKROLL = float(os.getenv("KELLY_BANKROLL", "50.0"))   # trading capital in USD to size against
    KELLY_MIN_BET  = float(os.getenv("KELLY_MIN_BET",  "1.0"))    # floor (Polymarket CLOB minimum)
    KELLY_MAX_BET  = float(os.getenv("KELLY_MAX_BET",  "10.0"))   # ceiling (safety cap)

    # Volume filter
    REQUIRE_VOL = os.getenv("REQUIRE_VOL", "true").lower() == "true"

    # Telegram alerting
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

    # Risk: max simultaneous open positions across all assets
    MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "2"))

    # Risk: global 8-hour drawdown stop (fraction of bankroll)
    MAX_GLOBAL_8H_LOSS_PCT = float(os.getenv("MAX_GLOBAL_8H_LOSS_PCT", "0.25"))

    # OBI veto thresholds per asset (absolute value; signal contradicted when exceeded).
    # Raised for ETH/XRP/SOL: Bybit books are structurally ask-heavy during rallies,
    # so tight thresholds produce false vetoes on genuine bullish moves.
    OBI_THRESHOLDS: dict = {
        "BTCUSDT": float(os.getenv("OBI_THRESHOLD_BTC", "0.20")),
        "ETHUSDT": float(os.getenv("OBI_THRESHOLD_ETH", "0.18")),
        "XRPUSDT": float(os.getenv("OBI_THRESHOLD_XRP", "0.18")),
        "SOLUSDT": float(os.getenv("OBI_THRESHOLD_SOL", "0.15")),
    }

    # OBI trend sensitivity: minimum per-sample improvement rate to classify OBI as
    # "recovering". When OBI contradicts direction but is recovering at or above this
    # rate, the veto is lifted (Change 2 — trend-aware suppression).
    OBI_RECOVERY_RATE: float = float(os.getenv("OBI_RECOVERY_RATE", "0.005"))

    # BTC-lag OBI relaxation multiplier: effective threshold is multiplied by this
    # factor when btc_lag is active and another asset confirms direction.
    # e.g. 1.4 → XRP effective threshold becomes 0.18 * 1.4 = 0.25 during lag moves.
    OBI_BTC_LAG_RELAX: float = float(os.getenv("OBI_BTC_LAG_RELAX", "1.4"))

    # Assets and symbols (can be expanded easily)
    ASSETS = ["BTCUSDT","ETHUSDT","XRPUSDT","SOLUSDT"]

    BYBIT_SYMBOLS = ["BTCUSD", "ETHUSD", "XRPUSD", "SOLUSD"]

    COINBASE_SYMBOLS = {
        "BTC-PERP": "BTC-PERP-INTX",
        "ETH-PERP": "ETH-PERP-INTX",
        "XRP-PERP": "XRP-PERP-INTX",
        "SOL-PERP": "SOL-PERP-INTX",
    }

    CHAINLINK_SYMBOLS = {
        "btc/usd": 0.0,
        "eth/usd": 0.0,
        "xrp/usd": 0.0,
        "sol/usd": 0.0,
    }
    WS_URL = "wss://ws-live-data.polymarket.com"
    CHAINLINK_FEED = "crypto_prices_chainlink"

    # ----------------------------------------------------------------------
    # Binance Spot — additional trigger source (parallel to Bybit)
    # ----------------------------------------------------------------------
    BINANCE_ENABLED = os.getenv("BINANCE_ENABLED", "true").lower() == "true"

    # Binance Spot symbols (USDT pairs — same string as ASSETS, conveniently)
    BINANCE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT"]

    # Above-average volume gate for Binance triggers.
    # Mirrors the existing Bybit VolumeTracker pattern:
    #   high_vol_minute = last_closed_1m_volume > mean(prev N 1m candles) * multiplier
    BINANCE_VOL_MULTIPLIER = float(os.getenv("BINANCE_VOL_MULTIPLIER", "1.25"))
    BINANCE_VOL_LOOKBACK   = int(os.getenv("BINANCE_VOL_LOOKBACK",   "10"))

    # Per-symbol Binance trigger throttle (seconds). Mirrors Bybit _last_trigger_ts.
    BINANCE_TRIGGER_THROTTLE_SEC = float(os.getenv("BINANCE_TRIGGER_THROTTLE_SEC", "5.0"))

    # ----------------------------------------------------------------------
    # Alignment gate: N-of-M direction agreement across trigger sources.
    # Sources considered: {Bybit Futures, Binance Spot, Coinbase Futures}.
    # Chainlink remains informational only (oracle is stale at ≤0.5% moves).
    # ----------------------------------------------------------------------
    ALIGNMENT_MIN_SOURCES = int(os.getenv("ALIGNMENT_MIN_SOURCES", "2"))   # 2-of-3
    # Minimum |pct| for a source to count as an active vote (was hardcoded 0.03)
    ALIGNMENT_MIN_PCT     = float(os.getenv("ALIGNMENT_MIN_PCT", "0.03"))

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
    