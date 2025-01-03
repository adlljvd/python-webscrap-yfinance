import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

def get_esg_score(ticker_symbol):
    try:
        # Construct URL
        url = f"https://finance.yahoo.com/quote/{ticker_symbol.strip()}/sustainability/"
        print(f"Accessing URL: {url}")
        
        # Headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Get the webpage
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Print the first part of the response to debug
        print("Response received. Searching for ESG data...")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try to find the script tag containing the ESG data
        scripts = soup.find_all('script')
        esg_data = None
        
        for script in scripts:
            if script.string and 'root.App.main' in script.string:
                data_str = script.string
                start_idx = data_str.find('{')
                end_idx = data_str.rfind('}') + 1
                json_data = json.loads(data_str[start_idx:end_idx])
                
                # Navigate through the JSON structure
                try:
                    esg_data = json_data['context']['dispatcher']['stores']['QuoteSummaryStore']['esgScores']
                    if esg_data and 'totalEsg' in esg_data:
                        total_esg = esg_data['totalEsg']['raw']
                        print(f"Found ESG score: {total_esg}")
                        return float(total_esg)
                except (KeyError, TypeError):
                    continue
        
        print(f"No ESG Risk Score found for {ticker_symbol}")
        return None
            
    except Exception as e:
        print(f"Error fetching ESG data for {ticker_symbol}: {str(e)}")
        return None

# Read tickers from CSV
tickers_df = pd.read_csv('ticker.csv')
tickers = [ticker.strip() for ticker in tickers_df['Ticker'].tolist()]

# Store results in a list
all_results = []

# Process each ticker
for ticker in tickers:
    print(f"\nFetching ESG Risk Score for {ticker}...")
    esg_score = get_esg_score(ticker)
    
    if esg_score is not None:
        result = {
            'Symbol': ticker,
            'ESG Risk Score': esg_score
        }
        all_results.append(result)
        
        # Print result for this ticker
        print(f"Stock: {ticker}")
        print(f"ESG Risk Score: {esg_score:.2f}")
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(2)

# Create a DataFrame with all results
df = pd.DataFrame(all_results)
if not df.empty:
    print("\nAll ESG Risk Scores:")
    print("=" * 50)
    print(df.to_string(index=False))
    
    # Save to CSV
    df.to_csv('esg_risk_scores.csv', index=False)
    print("\nESG Risk Scores have been saved to esg_risk_scores.csv")