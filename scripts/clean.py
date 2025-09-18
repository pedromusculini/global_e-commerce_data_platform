import os
import pandas as pd
from pathlib import Path

DATA_DIR = Path('../data')
OUTPUT_DIR = Path('../data/processed')
import os
import pandas as pd
from pathlib import Path

DATA_DIR = Path('../data')
OUTPUT_DIR = Path('../data/processed')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# List of international files to clean (adjust as needed)
FILES = [
    'E-commerce Dataset.csv',
    'online_retail.csv',
    'Sale Report.csv',
]

def clean_ecommerce(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['Customer_Id', 'Order_Date'])
    df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
    numeric_cols = ['Sales', 'Quantity', 'Discount', 'Profit', 'Shipping_Cost']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df[(df['Sales'] > 0) & (df['Quantity'] > 0)]
    categorical_cols = ['Gender', 'Device_Type', 'Product_Category']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')
    return df

def clean_online_retail(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['CustomerID', 'InvoiceDate'])
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
    df = df[df['Quantity'] > 0]
    return df

def clean_sale_report(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['Order_ID', 'Order_Date'])
    df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
    df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce')
    return df

cleaning_map = {
    'E-commerce Dataset.csv': clean_ecommerce,
    'online_retail.csv': clean_online_retail,
    'Sale Report.csv': clean_sale_report,
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

