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
OUTPUT_CSV = OUTPUT_DIR / "cotizaciones_live.csv"
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

def write_csv(df: pd.DataFrame, path: Path, sep: str = ';', decimal: str = ','):
    # sin columna "variacion_%"
    path.parent.mkdir(parents=True, exist_ok=True)  # asegura que exista archivos_salida
    cols = ["ticker", "nombre", "precio", "divisa", "hora", "fuente"]
    df[cols].to_csv(path, index=False, encoding="utf-8", sep=sep, decimal=decimal)
    print(f"[OK] Escrito {path} ({len(df)} filas).")

if __name__ == "__main__":
    tickers = load_tickers(JSON_TICKERS_PATH, TICKERS_KEY)
    df = fetch_prices(tickers)
    write_csv(df, OUTPUT_CSV)
