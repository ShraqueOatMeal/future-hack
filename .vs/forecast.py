from industry_map import industry_keywords, symbol_lookup 
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


def fetch_latest_stock_data(symbol="NVDA", interval="1day", points=12):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": points,
        "apikey": TWELVE_DATA_API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data["values"] if "values" in data else []


def format_stock_prompt(data_points):
    formatted = "\n".join([
        f"- {point['datetime']}: close={point['close']}, volume={point.get('volume', 'N/A')}"
        for point in data_points[::-1]  # oldest to newest
    ])
    return f"Based on the following stock data:\n{formatted}\n\nPredict the next closing price and explain your reasoning."


def query_groq_for_prediction(prompt):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    body = {
        "model": "llama3-70b-8192",
        "messages": [
            {"role": "system", "content": "You are a stock market analyst."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2
    }
    response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=body)
    result = response.json()
    return result['choices'][0]['message']['content']



def basic_xai_logic(values):
    closes = [float(p['close']) for p in values[-3:]]
    if closes[0] < closes[1] < closes[2]:
        return "XAI Insight: Detected 3 consecutive price increases — bullish trend."
    elif closes[0] > closes[1] > closes[2]:
        return "XAI Insight: 3 consecutive price drops — bearish pressure."
    else:
        return "XAI Insight: Recent trend is mixed or sideways."



# search for stock symbol using Twelve Data API
def search_symbol_twelve_data(query):
    import requests
    from dotenv import load_dotenv
    import os

    load_dotenv()
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    base_url = "https://api.twelvedata.com/symbol_search"
    params = {
        "symbol": query.strip().upper(),
        "apikey": api_key
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    # Return top matched symbol if found
    if "data" in data and len(data["data"]) > 0:
        return data["data"][0]["symbol"]
    return None



def resolve_symbols(query):
    query = query.lower()

    # Check for matching industry keyword
    for keyword, symbols in industry_keywords.items():
        if keyword in query:
            return symbols

    # Check symbol lookup
    for keyword, industry in symbol_lookup.items():
        if keyword in query:
            return industry_keywords.get(industry, [])

    # Try to match any single stock name via Twelve Data
    fallback_symbol = search_symbol_twelve_data(query)
    if fallback_symbol:
        return [fallback_symbol]  

    return []




def run_forecast_for_topic(query):
    symbols = resolve_symbols(query)
    if not symbols:
        return "No matching stocks found for the topic."

    all_data = []
    for symbol in symbols:
        data = fetch_latest_stock_data(symbol)
        if data:
            all_data.append((symbol, data))

    # Build prompt
    sections = []
    for symbol, data in all_data:
        formatted = format_stock_prompt(data)
        sections.append(f"Stock: {symbol}\n{formatted}")

    prompt = "Analyze the following companies related to the query:\n\n" + "\n\n".join(sections)
    prompt += f"\n\nUser query: {query}\nGive a short prediction and reasoning."

    forecast = query_groq_for_prediction(prompt)
    return forecast




