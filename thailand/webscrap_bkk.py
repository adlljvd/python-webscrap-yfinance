import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime
import time

def get_stock_metrics(ticker_symbol):
    try:
        # Get stock info
        stock = yf.Ticker(ticker_symbol.strip())  # Remove any whitespace
        info = stock.info
        
        if not info:
            print(f"No data found for {ticker_symbol}")
            return None

        # Extract the required information
        revenue = info.get('totalRevenue', 'N/A')
        revenue_in_billions = revenue / 1_000_000_000 if revenue != 'N/A' else 0

        profit_margin = info.get('profitMargins', 'N/A')
        profit_margin = profit_margin * 100 if profit_margin != 'N/A' else 0

        de_ratio = info.get('debtToEquity', 'N/A')
        de_ratio = de_ratio if de_ratio != 'N/A' else 0

        roe = info.get('returnOnEquity', 'N/A')
        roe = roe * 100 if roe != 'N/A' else 0

        beta = info.get('beta', 'N/A')
        beta = beta if beta != 'N/A' else 0

        div_yield = info.get('dividendYield', 'N/A')
        div_yield = div_yield * 100 if div_yield != 'N/A' else 0

        # Print raw data for debugging
        print(f"Raw data for {ticker_symbol}:")
        print(f"Revenue: {revenue}")
        print(f"Profit Margin: {info.get('profitMargins')}")
        print(f"D/E Ratio: {info.get('debtToEquity')}")
        print(f"ROE: {info.get('returnOnEquity')}")
        print(f"Beta: {info.get('beta')}")
        print(f"Dividend Yield: {info.get('dividendYield')}\n")

        return {
            'revenue_billions': revenue_in_billions,
            'profit_margin': profit_margin,
            'de_ratio': de_ratio,
            'roe': roe,
            'beta': beta,
            'div_yield': div_yield
        }
    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {str(e)}")
        return None

# Read tickers from CSV
tickers_df = pd.read_csv('ticker.csv')
tickers = [ticker.strip() for ticker in tickers_df['Ticker'].tolist()]

# Create SQLite database and table
conn = sqlite3.connect('stock_metrics.db')
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_metrics (
        date TEXT,
        stock_symbol TEXT,
        revenue_billions REAL,
        profit_margin REAL,
        de_ratio REAL,
        roe REAL,
        beta REAL,
        div_yield REAL
    )
''')

current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Process each ticker
for ticker in tickers:
    print(f"\nFetching data for {ticker}...")
    metrics = get_stock_metrics(ticker)
    
    if metrics:
        # Insert data
        cursor.execute('''
            INSERT INTO stock_metrics (
                date, stock_symbol, revenue_billions, profit_margin,
                de_ratio, roe, beta, div_yield
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            current_date, ticker, 
            metrics['revenue_billions'],
            metrics['profit_margin'],
            metrics['de_ratio'],
            metrics['roe'],
            metrics['beta'],
            metrics['div_yield']
        ))
        
        # Print results for this ticker
        print(f"Stock: {ticker}")
        print(f"Size (Revenue in B): {metrics['revenue_billions']:.2f}")
        print(f"Profit Margin %: {metrics['profit_margin']:.2f}")
        print(f"D/E %: {metrics['de_ratio']:.2f}")
        print(f"ROE %: {metrics['roe']:.2f}")
        print(f"Beta: {metrics['beta']:.2f}")
        print(f"Dividend Yield %: {metrics['div_yield']:.2f}")
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(2)

# Commit changes
conn.commit()

# Display all records from today
print("\nToday's records from database:")
print("Date                 | Symbol | Revenue | Profit% | D/E% | ROE% | Beta | Div%")
print("-" * 80)
cursor.execute('''
    SELECT * FROM stock_metrics 
    WHERE date LIKE ?
    ORDER BY stock_symbol
''', (current_date.split()[0] + '%',))

for row in cursor.fetchall():
    print(f"{row[0]} | {row[1]:6} | {row[2]:7.2f} | {row[3]:7.2f} | {row[4]:4.2f} | {row[5]:4.2f} | {row[6]:4.2f} | {row[7]:4.2f}")

conn.close()
print("\nData has been saved to stock_metrics.db")