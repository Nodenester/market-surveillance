# Market Surveillance

**Built: February 2026** | Archived

Real-time tick-level market surveillance tool designed to detect manipulation around Polymarket 5-minute BTC settlement windows. Monitors multiple exchanges simultaneously and correlates order flow to identify suspicious patterns.

## What It Does

Connects to four live data feeds and runs continuous analytics:

- **Binance WebSocket** -- individual trades (sub-100ms) + order book depth
- **Alpaca WebSocket** -- cross-exchange crypto trade comparison
- **Polymarket CLOB** -- YES/NO token orderbook + trades
- **Polymarket Gamma** -- automatic market discovery

### Analytics

- Cumulative Volume Delta (CVD): buy pressure vs sell pressure
- Order Book Imbalance (OBI): bid depth vs ask depth at best levels
- Volume acceleration per second (heatmap)
- Large trade detection and direction tracking
- Cross-exchange price divergence (Binance vs Alpaca)
- Polymarket token flow analysis (YES vs NO)
- Kill Zone analysis (last 30s of each 5-min window)
- Window-over-window pattern detection
- Manipulation prediction signal

## Tech Stack

- Python 3.12
- `websockets` -- real-time exchange feeds
- `rich` -- terminal dashboard with live updating panels
- `FastAPI` + `uvicorn` -- web dashboard backend
- `httpx` -- async HTTP for REST APIs
- Vanilla HTML/CSS/JS -- browser dashboard (JetBrains Mono, dark theme)

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env and add your Alpaca keys (optional, for cross-exchange comparison)
cp .env.example .env

# Terminal UI
python surveillance.py
python surveillance.py --coins BTC,ETH --timeframe 5m
python surveillance.py --no-ui  # headless logging only

# Web dashboard at http://localhost:7777
python web.py
```

Binance and Polymarket feeds require no API keys. Alpaca is optional for cross-exchange analysis.

## Author

NodeNestor

## License

MIT
