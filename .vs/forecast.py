from typing import Dict, List
from industry_map import industry_keywords, symbol_lookup
import requests
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = "https://api.x.ai/v1"
MODEL = "grok-3"  # Updated to Grok 3
client = OpenAI(api_key=GROQ_API_KEY, base_url=GROQ_API_URL)

# def fetch_latest_stock_data(symbol="NVDA", interval="1day", points=12):
#     url = "https://api.twelvedata.com/time_series"
#     params = {
#         "symbol": symbol,
#         "interval": interval,
#         "outputsize": points,
#         "apikey": TWELVE_DATA_API_KEY
#     }
#     response = requests.get(url, params=params)
#     data = response.json()
#     return data["values"] if "values" in data else []


# def format_stock_prompt(data_points):
#     formatted = "\n".join([
#         f"- {point['datetime']}: close={point['close']}, volume={point.get('volume', 'N/A')}"
#         for point in data_points[::-1]  # oldest to newest
#     ])
#     return f"Based on the following stock data:\n{formatted}\n\nPredict the next closing price and explain your reasoning."


# def query_groq_for_prediction(prompt):
#     headers = {
#         "Authorization": f"Bearer {GROQ_API_KEY}",
#         "Content-Type": "application/json"
#     }
#     body = {
#         "model": "grok-3",
#         "messages": [
#             {"role": "system", "content": "You are a stock market analyst."},
#             {"role": "user", "content": prompt}
#         ],
#         "temperature": 0.2
#     }
#     response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=body)
#     result = response.json()
#     return result['choices'][0]['message']['content']


# def basic_xai_logic(values):
#     closes = [float(p['close']) for p in values[-3:]]
#     if closes[0] < closes[1] < closes[2]:
#         return "XAI Insight: Detected 3 consecutive price increases — bullish trend."
#     elif closes[0] > closes[1] > closes[2]:
#         return "XAI Insight: 3 consecutive price drops — bearish pressure."
#     else:
#         return "XAI Insight: Recent trend is mixed or sideways."


# # search for stock symbol using Twelve Data API
# def search_symbol_twelve_data(query):
#     import requests
#     from dotenv import load_dotenv
#     import os

#     load_dotenv()
#     api_key = os.getenv("TWELVE_DATA_API_KEY")

#     base_url = "https://api.twelvedata.com/symbol_search"
#     params = {
#         "symbol": query.strip().upper(),
#         "apikey": api_key
#     }

#     response = requests.get(base_url, params=params)
#     data = response.json()

#     # Return top matched symbol if found
#     if "data" in data and len(data["data"]) > 0:
#         return data["data"][0]["symbol"]
#     return None


# def resolve_symbols(query):
#     query = query.lower()

#     # Check for matching industry keyword
#     for keyword, symbols in industry_keywords.items():
#         if keyword in query:
#             return symbols

#     # Check symbol lookup
#     for keyword, industry in symbol_lookup.items():
#         if keyword in query:
#             return industry_keywords.get(industry, [])

#     # Try to match any single stock name via Twelve Data
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

#     # Build prompt
#     sections = []
#     for symbol, data in all_data:
#         formatted = format_stock_prompt(data)
#         sections.append(f"Stock: {symbol}\n{formatted}")

#     prompt = "Analyze the following companies related to the query:\n\n" + "\n\n".join(sections)
#     prompt += f"\n\nUser query: {query}\nGive a short prediction and reasoning."

#     forecast = query_groq_for_prediction(prompt)
#     return forecast


industry_keywords = {
    "ev": ["TSLA", "NIO", "XPEV"],
    "tech": ["AAPL", "MSFT", "NVDA"],
    "energy": ["XOM", "CVX"],
}
symbol_lookup = {"tesla": "ev", "apple": "tech", "nvidia": "tech"}


def fetch_latest_stock_data(
    symbol: str = "NVDA", interval: str = "1day", points: int = 12
) -> Dict:
    """
    Fetch latest stock data from Twelve Data API for a given symbol.

    Args:
        symbol (str): Stock symbol (e.g., NVDA).
        interval (str): Time interval (e.g., 1day).
        points (int): Number of data points to fetch.

    Returns:
        Dict: Stock data or error details.
    """
    print(f"[INFO] Fetching stock data for {symbol}...")
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": points,
        "apikey": TWELVE_DATA_API_KEY,
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "values" in data:
            print(
                f"[INFO] Successfully fetched {len(data['values'])} data points for {symbol}"
            )
            return {"status": "ok", "symbol": symbol, "values": data["values"]}
        else:
            print(
                f"[ERROR] No values returned for {symbol}: {data.get('message', 'Unknown error')}"
            )
            return {
                "status": "error",
                "message": data.get("message", "No data available"),
                "suggestions": ["Check stock symbol", "Verify API key"],
            }
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch stock data for {symbol}: {e}")
        return {
            "status": "error",
            "message": str(e),
            "suggestions": [
                "Check stock symbol",
                "Verify API key",
                "Check internet connection",
            ],
        }


def format_stock_prompt(data_points: List[Dict]) -> str:
    """
    Format a prompt for Grok-3 to generate a stock forecast.

    Args:
        data_points (List[Dict]): List of stock data points.

    Returns:
        str: Formatted prompt for Grok-3.
    """
    print("[INFO] Formatting stock prompt...")
    formatted = "\n".join(
        [
            f"- {point['datetime']}: close={point['close']}, volume={point.get('volume', 'N/A')}"
            for point in data_points[::-1]  # Oldest to newest
        ]
    )
    prompt = f"Based on the following stock data:\n{formatted}\n\nPredict the next closing price and explain your reasoning."
    print(f"[INFO] Generated prompt: {prompt[:100]}...")  # Truncate for brevity
    return prompt


def query_groq_for_prediction(prompt: str) -> str:
    """
    Query Grok-3 for a stock price prediction.

    Args:
        prompt (str): The formatted prompt.

    Returns:
        str: Prediction or error message.
    """
    print("[INFO] Querying Grok-3 for prediction...")
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a stock market analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        prediction = response.choices[0].message.content.strip()
        print(
            f"[INFO] Grok-3 prediction: {prediction[:100]}..."
        )  # Truncate for brevity
        return prediction
    except Exception as e:
        print(f"[ERROR] Grok prediction failed: {e}")
        return f"Prediction unavailable due to API error: {str(e)}"


def basic_xai_logic(values: List[Dict]) -> str:
    """
    Generate a basic trend insight based on recent stock prices.

    Args:
        values (List[Dict]): List of stock data points.

    Returns:
        str: Trend insight.
    """
    print("[INFO] Analyzing stock trend with basic XAI logic...")
    if len(values) < 3:
        print("[WARNING] Insufficient data for trend analysis")
        return "XAI Insight: Insufficient data for trend analysis."
    closes = [float(p["close"]) for p in values[-3:]]
    if closes[0] < closes[1] < closes[2]:
        print("[INFO] Detected bullish trend")
        return "XAI Insight: Detected 3 consecutive price increases — bullish trend."
    elif closes[0] > closes[1] > closes[2]:
        print("[INFO] Detected bearish trend")
        return "XAI Insight: 3 consecutive price drops — bearish pressure."
    else:
        print("[INFO] Detected mixed or sideways trend")
        return "XAI Insight: Recent trend is mixed or sideways."


def search_symbol_twelve_data(query: str) -> str:
    """
    Search for a stock symbol using Twelve Data API.

    Args:
        query (str): The query to search for.

    Returns:
        str: Top matched symbol or None.
    """
    print(f"[INFO] Searching for stock symbol: {query}...")
    base_url = "https://api.twelvedata.com/symbol_search"
    params = {"symbol": query.strip().upper(), "apikey": TWELVE_DATA_API_KEY}
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "data" in data and len(data["data"]) > 0:
            symbol = data["data"][0]["symbol"]
            print(f"[INFO] Found symbol: {symbol}")
            return symbol
        print("[WARNING] No symbol found")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Symbol search failed for {query}: {e}")
        return None


def resolve_symbols(query: str) -> List[str]:
    """
    Resolve stock symbols from the query using industry keywords or Twelve Data API.

    Args:
        query (str): The user's query.

    Returns:
        List[str]: List of resolved stock symbols.
    """
    print("[INFO] Resolving stock symbols from query...")
    query_lower = query.lower()

    # Check for matching industry keyword
    for keyword, symbols in industry_keywords.items():
        if keyword in query_lower:
            print(f"[INFO] Matched industry keyword: {keyword} -> {symbols}")
            return symbols

    # Check symbol lookup
    for keyword, industry in symbol_lookup.items():
        if keyword in query_lower:
            symbols = industry_keywords.get(industry, [])
            print(f"[INFO] Matched symbol lookup: {keyword} -> {symbols}")
            return symbols

    # Try to match any single stock name via Twelve Data
    fallback_symbol = search_symbol_twelve_data(query)
    if fallback_symbol:
        print(f"[INFO] Fallback symbol from Twelve Data: {fallback_symbol}")
        return [fallback_symbol]

    print("[WARNING] No stock symbols resolved")
    return []


def run_forecast_for_topic(query: str) -> Dict:
    """
    Generate a stock forecast for the given query.

    Args:
        query (str): The user's query containing stock symbols or topics.

    Returns:
        Dict: Forecast results with symbols, predictions, and summary.
    """
    print("[INFO] Starting forecast generation...")
    symbols = resolve_symbols(query)
    if not symbols:
        print("[ERROR] No matching stocks found for the topic")
        return {
            "success": False,
            "message": "No matching stocks found for the topic",
            "suggestions": [
                "Include a stock symbol (e.g., AAPL)",
                "Try a clearer query",
            ],
        }

    results = []
    for symbol in symbols:
        data = fetch_latest_stock_data(symbol)
        if data.get("status") == "ok":
            formatted = format_stock_prompt(data["values"])
            trend = basic_xai_logic(data["values"])
            prompt = f"Stock: {symbol}\n{formatted}\n{trend}\n\nUser query: {query}\nGive a short prediction and reasoning."
            prediction = query_groq_for_prediction(prompt)
            results.append(
                {
                    "symbol": symbol,
                    "data": data["values"],
                    "trend": trend,
                    "prediction": prediction,
                }
            )
        else:
            results.append(
                {
                    "symbol": symbol,
                    "data": [],
                    "trend": "No data available",
                    "prediction": data.get("message", "No data available"),
                }
            )

    if not results:
        print("[ERROR] No stock data available for the specified topic")
        return {
            "success": False,
            "message": "No stock data available for the specified topic",
            "suggestions": [
                "Check stock symbol validity",
                "Verify API key",
                "Try a different topic",
            ],
        }

    summary = "\n".join([f"{r['symbol']}: {r['prediction']}" for r in results])
    print(f"[INFO] Forecast summary: {summary}")
    return {"success": True, "symbols": symbols, "results": results, "summary": summary}

