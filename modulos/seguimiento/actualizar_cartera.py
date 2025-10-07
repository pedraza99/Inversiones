from pathlib import Path
from datetime import datetime
import yfinance as yf
import pandas as pd
import pytz

from funciones_auxiliares_seguimiento import safe_get_price, load_tickers

# Zona horaria
TZ = pytz.timezone("Europe/Madrid")

# Rutas (parten de la raíz del repo)
ROOT = Path(__file__).resolve().parents[2]
JSON_TICKERS_PATH = ROOT / "datos" / "tikr.json"                   # lee el JSON
OUTPUT_DIR = ROOT / "modulos" / "seguimiento" / "archivos_salida"  # <-- nueva carpeta de salida
OUTPUT_CSV_STD = OUTPUT_DIR / "cotizaciones_live.csv"      # ,  y .
OUTPUT_CSV_ES  = OUTPUT_DIR / "cotizaciones_live_es.csv"   # ;  y ,
TICKERS_KEY = "TICKERS_Alberto"                                    # <-- lee esta clave del JSON


def fetch_prices(tickers: dict) -> pd.DataFrame:
    rows = []
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    for tk, name in tickers.items():
        try:
            price, ccy, _ = safe_get_price(tk)
            rows.append({
                "ticker": tk,
                "nombre": name,
                "precio": round(price, 4),
                "divisa": ccy,
                "hora": now,
                "fuente": "Yahoo Finance (yfinance; posible retraso)",
            })
        except Exception as e:
            rows.append({
                "ticker": tk,
                "nombre": name,
                "precio": None,
                "divisa": None,
                "hora": now,
                "fuente": f"ERROR: {e}",
            })
    return pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)

def write_csvs(df: pd.DataFrame):
    cols = ["ticker", "nombre", "precio", "divisa", "hora", "fuente"]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) CSV para GitHub (render tabla)
    df[cols].to_csv(OUTPUT_CSV_STD, index=False, encoding="utf-8")

    # 2) CSV “ES” para Sheets/Apps Script (coma decimal)
    df[cols].to_csv(OUTPUT_CSV_ES, index=False, encoding="utf-8", sep=';', decimal=',')

    print(f"[OK] Escritos:\n- {OUTPUT_CSV_STD}\n- {OUTPUT_CSV_ES}")

if __name__ == "__main__":
    tickers = load_tickers(JSON_TICKERS_PATH, TICKERS_KEY)
    df = fetch_prices(tickers)
    write_csvs(df)