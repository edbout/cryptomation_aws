# Polymarket Trading Bot — Overview
What it does: Trades Polymarket 5-minute binary up/down markets for BTC, ETH, XRP, SOL using a momentum strategy.

# Architecture: 
Three WebSocket feeds (Bybit inverse futures, Coinbase PERP, Chainlink via Polymarket) provide cross-validated 5-minute momentum signals. All trading is 100% event-driven through Bybit ticker ticks — the timer loop only handles redemptions and token pre-approvals. When Bybit fires a tick and all three sources agree on direction with OBI confirmation, the bot places a Kelly-sized order on the corresponding Polymarket YES or NO 5m binary. It then manages the open position every 5 seconds with TP/SL/trailing stop logic. After the 5m market closes, the redeem job at M:00s:10 collects winnings.

# Key Components
    File	Role
    [main.py](Cryptomation AWSn/main.py)	Orchestration, feeds, timer loop
    [lib/order_manager.py](Cryptomation AWSn/lib/order_manager.py)	Order placement & approvals
    [price_tracker.py](Cryptomation AWSn/price_tracker.py)	Signal recording, stats, fair-value lookups
    [calibrate_params.py](Cryptomation AWSn/calibrate_params.py)	Offline parameter tuning
    [redeem.py](Cryptomation AWSn/redeem.py)	Position redemption at expiry
    [config.py](Cryptomation AWSn/config.py)	Config dataclass + Redis singleton

# Infrastructure
    Redis Cloud — state, signals, order tracking, stats, caching
    Polygon RPC — balance/allowance reads with 7-provider failover
    AWS — deployment target (also runs locally)
    DRY_RUN=true currently set — no live trades being placed

