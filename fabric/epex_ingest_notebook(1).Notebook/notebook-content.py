# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8833f0f-f41a-462e-9bb4-11b3fa42d03b",
# META       "default_lakehouse_name": "EpexLH",
# META       "default_lakehouse_workspace_id": "87da83ca-2469-42d1-849f-be8b17bc55ba",
# META       "known_lakehouses": [
# META         {
# META           "id": "e8833f0f-f41a-462e-9bb4-11b3fa42d03b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # EPEX Spot Day-Ahead Fiyat Ingestion
# 
# Bu notebook EPEX Spot gün öncesi elektrik fiyatlarını çeker ve `epex_spot_prices` Delta tablosuna yazar.
# 
# **Kullanım:**
# 1. Notebook'u `EpexLH` lakehouse'una bağla (sol panelden Add data items > Existing lakehouse)
# 2. İlk hücreyi *parameter cell* yap (hücre `...` menüsü > Toggle parameter cell)
# 3. Çalıştır. Scraping başarısız olursa otomatik simüle veri üretir (`USE_SIMULATED_FALLBACK = True` ise).

# PARAMETERS CELL ********************

market_area = "FI"
USE_SIMULATED_FALLBACK = True
BACKFILL_DAYS = 30        # 0 = sadece yarın; >0 ise geçmiş N gün simüle backfill (ilk kurulum için 30 önerilir)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from bs4 import BeautifulSoup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %pip install beautifulsoup4 --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date, datetime, timedelta, timezone
from typing import Any
import random
import math

import pandas as pd
import requests
from bs4 import BeautifulSoup
from delta.tables import *



def _to_float(v: str) -> float:
    return float(v.replace(",", ""))

def _as_date_str(v: date) -> str:
    return v.strftime("%Y-%m-%d")

def extract_invokes(data) -> dict:
    invokes = {}
    for entry in data:
        if entry.get("command") == "invoke":
            invokes[entry["selector"]] = entry
    return invokes

def fetch_data(delivery_date: date, market_area: str) -> dict:
    trading_date = delivery_date - timedelta(days=1)
    params = {
        "market_area": market_area,
        "trading_date": _as_date_str(trading_date),
        "delivery_date": _as_date_str(delivery_date),
        "modality": "Auction",
        "sub_modality": "DayAhead",
        "product": "60",
        "data_mode": "table",
        "ajax_form": 1,
    }
    data = {
        "form_id": "market_data_filters_form",
        "_triggering_element_name": "submit_js",
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.post("https://www.epexspot.com/en/market-data", params=params, data=data, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_table_data(delivery_date: datetime, data: dict, market_area: str):
    soup = BeautifulSoup(data["args"][0], features="html.parser")
    try:
        table = soup.find("table", class_="table-01 table-length-1")
        body = table.tbody
        rows = body.find_all_next("tr")
    except AttributeError:
        return []

    start_time = delivery_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = start_time.astimezone(timezone.utc)

    records = []
    for row in rows:
        end_time = start_time + timedelta(hours=1)
        buy_volume_col = row.td
        sell_volume_col = buy_volume_col.find_next_sibling("td")
        volume_col = sell_volume_col.find_next_sibling("td")
        price_col = volume_col.find_next_sibling("td")
        records.append((
            market_area, start_time, end_time,
            _to_float(buy_volume_col.string),
            _to_float(sell_volume_col.string),
            _to_float(volume_col.string),
            _to_float(price_col.string),
        ))
        start_time = end_time

    return pd.DataFrame.from_records(records, columns=["market", "start_time", "end_time", "buy_volume", "sell_volume", "volume", "price"])

def fetch_day(delivery_date: datetime, market_area: str):
    data = fetch_data(delivery_date.date(), market_area)
    invokes = extract_invokes(data)
    table_data = invokes.get(".js-md-widget")
    if table_data is None:
        return []
    return extract_table_data(delivery_date, table_data, market_area)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- FALLBACK: simüle veri üretici ---
# Gerçekçi gün içi fiyat eğrisi: gece düşük, sabah/akşam pik, öğlen güneş etkisiyle düşüş

MARKET_BASE_PRICE = {
    "AT": 95, "BE": 90, "CH": 100, "DE-LU": 85, "DK1": 70, "DK2": 72,
    "FI": 55, "FR": 88, "GB": 105, "NL": 92, "NO1": 45, "NO2": 48,
    "NO3": 35, "NO4": 30, "NO5": 44, "PL": 98,
    "SE1": 38, "SE2": 40, "SE3": 55, "SE4": 62,
}

def simulate_day(delivery_date: datetime, market_area: str) -> pd.DataFrame:
    base = MARKET_BASE_PRICE.get(market_area, 80)
    # gün bazlı deterministik seed -> aynı gün için hep aynı veri (merge idempotent kalır)
    rng = random.Random(f"{market_area}-{delivery_date.date()}")
    day_factor = rng.uniform(0.8, 1.25)

    start_time = delivery_date.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    records = []
    for h in range(24):
        end_time = start_time + timedelta(hours=1)
        # çift tepe (duck curve benzeri): sabah 8 ve akşam 19 pik, gece ve öğlen düşük
        intraday = (
            1.0
            + 0.35 * math.exp(-((h - 8) ** 2) / 8)
            + 0.45 * math.exp(-((h - 19) ** 2) / 6)
            - 0.30 * math.exp(-((h - 13) ** 2) / 10)
            - 0.25 * math.exp(-((h - 3) ** 2) / 12)
        )
        noise = rng.uniform(-8, 8)
        price = round(base * day_factor * intraday + noise, 2)
        volume = round(rng.uniform(2000, 9000), 1)
        records.append((
            market_area, start_time, end_time,
            round(volume * rng.uniform(0.4, 0.6), 1),
            round(volume * rng.uniform(0.4, 0.6), 1),
            volume, price,
        ))
        start_time = end_time

    return pd.DataFrame.from_records(records, columns=["market", "start_time", "end_time", "buy_volume", "sell_volume", "volume", "price"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Tablo yoksa oluştur (ilk çalıştırma) ---
table_exists = spark.catalog.tableExists("epex_spot_prices")

def get_day_data(delivery_date: datetime, market_area: str) -> pd.DataFrame:
    """Önce scraping dene, başarısızsa fallback."""
    try:
        prices = fetch_day(delivery_date, market_area)
        if len(prices) > 0:
            print(f"[OK] Scraping başarılı: {market_area} {delivery_date.date()}")
            return prices
        print(f"[!] Veri henüz yayınlanmamış: {market_area} {delivery_date.date()}")
    except Exception as e:
        print(f"[!] Scraping hatası: {type(e).__name__}: {e}")

    if USE_SIMULATED_FALLBACK:
        print(f"[SIM] Simüle veri üretiliyor: {market_area} {delivery_date.date()}")
        return simulate_day(delivery_date, market_area)
    return pd.DataFrame()

# İngest edilecek günler: yarın + (varsa) geçmiş backfill
days_to_ingest = [datetime.now() + timedelta(days=1)]
for d in range(1, BACKFILL_DAYS + 1):
    days_to_ingest.append(datetime.now() - timedelta(days=d))

all_frames = []
for day in days_to_ingest:
    df = get_day_data(day, market_area)
    if len(df) > 0:
        all_frames.append(df)

if not all_frames:
    mssparkutils.notebook.exit("No prices available yet")

prices = pd.concat(all_frames, ignore_index=True)
spark_df = spark.createDataFrame(prices)
print(f"Toplam {spark_df.count()} satır hazır")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not table_exists:
    # İlk çalıştırma: tabloyu oluştur, market'e göre partition'la
    (spark_df.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .partitionBy("market")
        .saveAsTable("epex_spot_prices"))
    print("Tablo oluşturuldu: epex_spot_prices")
else:
    # Sonraki çalıştırmalar: merge / upsert (sadece yeni satırlar eklenir)
    current = DeltaTable.forName(spark, "EpexLH.dbo.epex_spot_prices")
    (current.alias("current")
        .merge(
            spark_df.alias("new"),
            f"current.market = '{market_area}' AND current.market = new.market AND current.start_time = new.start_time AND current.end_time = new.end_time"
        )
        .whenNotMatchedInsertAll()
        .execute())
    print("Merge tamamlandı")

# Kontrol
spark.sql(f"SELECT market, COUNT(*) as rows, MIN(start_time) as min_t, MAX(start_time) as max_t FROM epex_spot_prices WHERE market = '{market_area}' GROUP BY market").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT market, COUNT(*) as rows FROM EpexLH.dbo.epex_spot_prices GROUP BY market ORDER BY market").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
