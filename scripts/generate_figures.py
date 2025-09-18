"""Generate core figures required by the comparative report.

This script is idempotent: it will regenerate PNGs under reports/figures.
It expects that normalized and enriched data already exist.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

ROOT = Path(__file__).resolve().parent.parent
DATA_NORM = ROOT / 'data' / 'normalized'
DATA_ENRICHED = ROOT / 'data' / 'enriched'
FIG_DIR = ROOT / 'reports' / 'figures'
FIG_DIR.mkdir(parents=True, exist_ok=True)

PRODUCTS_PATH = DATA_NORM / 'products.parquet'
ORDERS_PATH = DATA_NORM / 'orders.parquet'
PRODUCTS_ENRICHED_PATH = DATA_ENRICHED / 'products_enriched.parquet'
ORDERS_ENRICHED_PATH = DATA_ENRICHED / 'orders_enriched.parquet'


def load(path: Path):
    if path.exists():
        if path.suffix == '.parquet':
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)
    return None

products = load(PRODUCTS_PATH)
orders = load(ORDERS_PATH)
products_enriched = load(PRODUCTS_ENRICHED_PATH)
orders_enriched = load(ORDERS_ENRICHED_PATH)

# 1. Product Price Distributions
if products is not None and len(products) > 0:
    price_col = 'price'
    if 'price_amount' in products.columns:
        price_col = 'price_amount'
    pnum = pd.to_numeric(products[price_col], errors='coerce')
    pnum = pnum[pnum.notna()]
    if not pnum.empty:
        plt.figure(figsize=(10,4))
        sns.histplot(pnum, bins=40, kde=True, color='#2563eb')
        plt.title('Product Price Distribution')
        plt.xlabel('Price')
        plt.tight_layout()
        outfile = FIG_DIR / 'products_price_distributions.png'
        plt.savefig(outfile, dpi=130)
        plt.close()

# 2. Orders Time Series & AOV Distribution
if orders is not None and len(orders) > 0:
    # Detect date column
    date_col = None
    for c in ['created_at','order_date','date','timestamp']:
        if c in orders.columns:
            date_col = c; break
    val_col = None
    for c in ['total_price','total','amount','grand_total','price']:
        if c in orders.columns:
            val_col = c; break
    if date_col:
        od = orders[[date_col]].copy()
        od[date_col] = pd.to_datetime(od[date_col], errors='coerce')
        daily = od.groupby(od[date_col].dt.date).size()
        if not daily.empty:
            plt.figure(figsize=(10,4))
            daily.plot(marker='o')
            plt.title('Orders Count per Day')
            plt.ylabel('Orders')
            plt.xlabel('Date')
            plt.tight_layout()
            plt.savefig(FIG_DIR / 'orders_time_series.png', dpi=130)
            plt.close()
    if val_col:
        v = pd.to_numeric(orders[val_col], errors='coerce').dropna()
        if not v.empty:
            plt.figure(figsize=(10,4))
            sns.histplot(v, bins=40, kde=True, color='#7c3aed')
            plt.title('Order AOV Distribution')
            plt.xlabel(val_col)
            plt.tight_layout()
            plt.savefig(FIG_DIR / 'orders_aov_distribution.png', dpi=130)
            plt.close()

print('Figures generated under', FIG_DIR)
