# run_cartera.py
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz
import time

# === CONFIGURA AQUÍ TUS TICKERS (Yahoo Finance) ===
TICKERS = {
    "META": "Meta Platforms (US)",
    "BXP": "BXP Inc (US)",
    "GOOGL": "Alphabet Class A (US)",
    "GOOG": "Alphabet Class C (US)",
    "PLMR": "Palomar (US)",
    "PYPL": "Paypal (US)",
    "AIR.PA": "Airbus (Francia)",
    "VFC": "VF Corp (US)",
    "UPWK": "Upwork (US)",
    "MTCH": "Match Group (US)",
    "STNE": "StoneCo (US ADR)",
    "CELH": "Celsius (US)",
    "NKE": "Nike (US)",
    "SMCI": "Super Micro Computer (US)",
    "ZEG.L": "Zegona Communications (UK)",
    "MC.PA": "LVMH (Francia)",
    "ASML.AS": "ASML (Países Bajos)",
    "GOOS": "Canada Goose (US)",
    "CSU.TO": "Constellation Software (Canadá)"
}

OUTPUT_CSV = "cotizaciones_live.csv"
TZ = pytz.timezone("Europe/Madrid")

def safe_get_price(ticker: str, retries: int = 3, sleep_s: float = 1.5):
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
                    return float(price), currency, (float(prev_close) if prev_close is not None else None)
            info = getattr(t, "info", {}) or {}
            price = info.get("regularMarketPrice") or info.get("currentPrice")
            prev_close = info.get("regularMarketPreviousClose")
            currency = info.get("currency")
            if price is not None and currency is not None:
                return float(price), currency, (float(prev_close) if prev_close is not None else None)
            hist = t.history(period="1d", interval="1m")
            if not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
                currency = info.get("currency") or "N/A"
                return price, currency, (float(prev_close) if prev_close is not None else None)
        except Exception as e:
            last_err = e
        time.sleep(sleep_s)
    raise RuntimeError(f"No se pudo obtener {ticker}: {last_err}")

def fetch_prices(tickers: dict) -> pd.DataFrame:
    rows = []
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    for tk, name in tickers.items():
        try:
            price, ccy, prev = safe_get_price(tk)
            change_pct = (price/prev - 1.0)*100.0 if prev else None
            rows.append({
                "ticker": tk,
                "nombre": name,
                "precio": round(price, 4),
                "divisa": ccy,
                "variacion_%": round(change_pct, 3) if change_pct is not None else None,
                "hora": now,
                "fuente": "Yahoo Finance (yfinance; posible retraso)",
            })
        except Exception as e:
            rows.append({
                "ticker": tk,
                "nombre": name,
                "precio": None,
                "divisa": None,
                "variacion_%": None,
                "hora": now,
                "fuente": f"ERROR: {e}",
            })
    df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
    return df

def write_csv(df: pd.DataFrame, path: str):
    cols = ["ticker","nombre","precio","divisa","variacion_%","hora","fuente"]
    out = df[cols]
    out.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] Escrito {path} con {len(out)} filas.")

if __name__ == "__main__":
    df_now = fetch_prices(TICKERS)
    write_csv(df_now, OUTPUT_CSV)
