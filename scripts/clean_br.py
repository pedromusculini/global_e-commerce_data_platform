import os
import pandas as pd
from pathlib import Path

DATA_DIR = Path('../data/brazil')
OUTPUT_DIR = Path('../data/processed/brazil')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = [
    'olist_orders_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_products_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_geolocation_dataset.csv',
    'product_category_name_translation.csv',
]

def clean_orders(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp'])
    for col in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_order_items(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['order_id', 'order_item_id', 'product_id', 'seller_id', 'price'])
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce')
    return df

def clean_customers(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['customer_id', 'customer_unique_id'])
    return df

def clean_payments(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['order_id', 'payment_type', 'payment_value'])
    df['payment_value'] = pd.to_numeric(df['payment_value'], errors='coerce')
    return df

def clean_reviews(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['order_id', 'review_id', 'review_score'])
    df['review_score'] = pd.to_numeric(df['review_score'], errors='coerce')
    if 'review_creation_date' in df.columns:
        df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    if 'review_answer_timestamp' in df.columns:
        df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
    return df

def clean_products(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['product_id', 'product_category_name'])
    return df

def clean_sellers(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['seller_id'])
    return df

def clean_geolocation(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'])
    df['geolocation_lat'] = pd.to_numeric(df['geolocation_lat'], errors='coerce')
    df['geolocation_lng'] = pd.to_numeric(df['geolocation_lng'], errors='coerce')
    return df

def clean_category_translation(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['product_category_name', 'product_category_name_english'])
    return df

cleaning_map = {
    'olist_orders_dataset.csv': clean_orders,
    'olist_order_items_dataset.csv': clean_order_items,
    'olist_customers_dataset.csv': clean_customers,
    'olist_order_payments_dataset.csv': clean_payments,
    'olist_order_reviews_dataset.csv': clean_reviews,
    'olist_products_dataset.csv': clean_products,
    'olist_sellers_dataset.csv': clean_sellers,
    'olist_geolocation_dataset.csv': clean_geolocation,
    'product_category_name_translation.csv': clean_category_translation,
}

def main():
    for file in FILES:
        input_path = DATA_DIR / file
        output_path = OUTPUT_DIR / file.replace('.csv', '_clean.csv')
        print(f'Processing {file}...')
        try:
            df = pd.read_csv(input_path, low_memory=False)
            df_clean = cleaning_map[file](df)
            df_clean.to_csv(output_path, index=False)
            print(f'Saved cleaned file to {output_path}')
        except Exception as e:
            print(f'Error processing {file}: {e}')

if __name__ == '__main__':
    main()
