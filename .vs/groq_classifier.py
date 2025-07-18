import re
import requests
from datetime import datetime
import os
from dotenv import load_dotenv
from openai import OpenAI
from forecast import fetch_latest_stock_data, resolve_symbols, format_stock_prompt, query_groq_for_prediction, run_forecast_for_topic

load_dotenv()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
if not XAI_API_KEY:
    raise ValueError("XAI_API_KEY not found")
client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")
MODEL = "grok-3"

# # Mock industry map (replace with actual industry_map.py)
# industry_keywords = {
#     "ev": ["TSLA", "NIO", "XPEV"],
#     "tech": ["AAPL", "MSFT", "NVDA"],
#     "energy": ["XOM", "CVX"]
# }
# symbol_lookup = {
#     "tesla": "ev",
#     "apple": "tech",
#     "nvidia": "tech"
# }

# def fetch_latest_stock_data(symbol="NVDA", interval="1day", points=12):
#     url = "https://api.twelvedata.com/time_series"
#     params = {
#         "symbol": symbol,
#         "interval": interval,
#         "outputsize": points,
#         "apikey": TWELVE_DATA_API_KEY
#     }
#     try:
#         response = requests.get(url, params=params)
#         response.raise_for_status()
#         data = response.json()
#         return data["values"] if "values" in data else []
#     except Exception as e:
#         print(f"[ERROR] Failed to fetch stock data for {symbol}: {e}")
#         return []

# def format_stock_prompt(data_points):
#     formatted = "\n".join([
#         f"- {point['datetime']}: close={point['close']}, volume={point.get('volume', 'N/A')}"
#         for point in data_points[::-1]
#     ])
#     return f"Based on the following stock data:\n{formatted}\n\nPredict the next closing price and explain your reasoning."

# def query_groq_for_prediction(prompt):
#     try:
#         response = client.chat.completions.create(
#             model=MODEL,
#             messages=[
#                 {"role": "system", "content": "You are a stock market analyst."},
#                 {"role": "user", "content": prompt}
#             ],
#             temperature=0.2
#         )
#         return response.choices[0].message.content
#     except Exception as e:
#         print(f"[ERROR] Groq prediction failed: {e}")
#         return "Prediction unavailable due to API error."

# def basic_xai_logic(values):
#     if len(values) < 3:
#         return "XAI Insight: Insufficient data for trend analysis."
#     closes = [float(p['close']) for p in values[-3:]]
#     if closes[0] < closes[1] < closes[2]:
#         return "XAI Insight: Detected 3 consecutive price increases — bullish trend."
#     elif closes[0] > closes[1] > closes[2]:
#         return "XAI Insight: 3 consecutive price drops — bearish pressure."
#     else:
#         return "XAI Insight: Recent trend is mixed or sideways."

# def search_symbol_twelve_data(query):
#     base_url = "https://api.twelvedata.com/symbol_search"
#     params = {
#         "symbol": query.strip().upper(),
#         "apikey": TWELVE_DATA_API_KEY
#     }
#     try:
#         response = requests.get(base_url, params=params)
#         response.raise_for_status()
#         data = response.json()
#         if "data" in data and len(data["data"]) > 0:
#             return data["data"][0]["symbol"]
#         return None
#     except Exception as e:
#         print(f"[ERROR] Symbol search failed for {query}: {e}")
#         return None

# def resolve_symbols(query):
#     query = query.lower()
#     for keyword, symbols in industry_keywords.items():
#         if keyword in query:
#             return symbols
#     for keyword, industry in symbol_lookup.items():
#         if keyword in query:
#             return industry_keywords.get(industry, [])
#     fallback_symbol = search_symbol_twelve_data(query)
#     if fallback_symbol:
#         return [fallback_symbol]
#     return []

# def run_forecast_for_topic(query):
#     symbols = resolve_symbols(query)
#     if not symbols:
#         return "No matching stocks found for the topic."
    
#     all_data = []
#     for symbol in symbols:
#         data = fetch_latest_stock_data(symbol)
#         if data:
#             all_data.append((symbol, data))
    
#     if not all_data:
#         return "No stock data available for the specified topic."
    
#     sections = []
#     for symbol, data in all_data:
#         formatted = format_stock_prompt(data)
#         trend = basic_xai_logic(data)
#         sections.append(f"Stock: {symbol}\n{formatted}\n{trend}")
    
#     prompt = "Analyze the following companies related to the query:\n\n" + "\n\n".join(sections)
#     prompt += f"\n\nUser query: {query}\nGive a short prediction and reasoning."
    
#     forecast = query_groq_for_prediction(prompt)
#     return forecast

def classify_intent_with_groq(user_query: str) -> str:
    print("[INFO] Classifying query intent...")
    
    # Expanded pattern-based classification
    patterns = {
        "PRODUCTIVITY": r'\b(productivity|efficiency|performance|output|hours worked)\b',
        "REVENUE": r'\b(revenue|sales|income|earnings)\b',
        "CUSTOMER_SATISFACTION": r'\b(customer satisfaction|feedback|reviews|ratings)\b',
        "INVENTORY": r'\b(inventory|stock|supply|warehouse)\b',
        "STOCK": r'\b(stock|market|forecast|price|trend|earnings)\b',
        "HYBRID": r'\b(stock|market|forecast)\b.*\b(sales|revenue|inventory|productivity)\b',
        "SIMPLE": r'\b(show|list|get|display)\b'
    }
    
    query_lower = user_query.lower()
    matched_intents = [intent for intent, pattern in patterns.items() if re.search(pattern, query_lower)]
    
    # Use Grok-3 for classification
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Classify the intent of the query as one of: SIMPLE (basic data retrieval), "
                        "STOCK (stock market data), HYBRID (internal + external data), "
                        "PRODUCTIVITY (employee performance), REVENUE (sales data), "
                        "CUSTOMER_SATISFACTION (customer metrics), INVENTORY (stock levels). "
                        "Return only the intent type in uppercase."
                    )
                },
                {"role": "user", "content": user_query}
            ],
            temperature=0.2
        )
        intent = response.choices[0].message.content.strip().upper()
        if intent in patterns:
            print(f"[INFO] Grok-3 classified intent: {intent}")
            return intent
    except Exception as e:
        print(f"[ERROR] Grok-3 classification failed: {e}. Using pattern-based fallback.")
    
    # Fallback to pattern-based classification
    if "HYBRID" in matched_intents:
        return "HYBRID"
    elif matched_intents:
        return matched_intents[0].upper()
    return "SIMPLE"