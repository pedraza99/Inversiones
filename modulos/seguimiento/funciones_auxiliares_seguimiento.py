import time
import yfinance as yf
import json
from pathlib import Path

def safe_get_price(ticker: str, retries: int = 3, sleep_s: float = 1.5):
    """
    Devuelve (precio, divisa, previous_close|None) para un ticker usando yfinance.
    Reintenta con pequeñas esperas.
    """
    last_err = None
    t = yf.Ticker(ticker)
    for _ in range(retries):
        try:
            fi = getattr(t, "fast_info", None)
            if fi:
                price = fi.get("last_price")
                prev_close = fi.get("previous_close")
                currency = fi.get("currency")
                if price is not None and currency is not None:
                    return float(price), str(currency), (float(prev_close) if prev_close is not None else None)

            info = getattr(t, "info", {}) or {}
            price = info.get("regularMarketPrice") or info.get("currentPrice")
            prev_close = info.get("regularMarketPreviousClose")
            currency = info.get("currency")
            if price is not None and currency is not None:
                return float(price), str(currency), (float(prev_close) if prev_close is not None else None)

            hist = t.history(period="1d", interval="1m")
            if not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
                currency = info.get("currency") or "N/A"
                return price, str(currency), (float(prev_close) if prev_close is not None else None)
        except Exception as e:
            last_err = e
        time.sleep(sleep_s)
    raise RuntimeError(f"No se pudo obtener {ticker}: {last_err}")


def load_tickers(json_path: Path, key: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if key not in data:
        raise KeyError(f"Clave '{key}' no encontrada en {json_path.name}. Claves: {list(data.keys())}")
    d = data[key]
    if not isinstance(d, dict) or not d:
        raise ValueError(f"'{key}' debe ser un dict {{ticker: nombre}}.")
    return {str(k).strip(): str(v).strip() for k, v in d.items()}