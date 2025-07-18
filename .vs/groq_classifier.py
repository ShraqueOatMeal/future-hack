import re
import os
from dotenv import load_dotenv
from openai import OpenAI
from forecast import (
    fetch_latest_stock_data,
    resolve_symbols,
    format_stock_prompt,
    query_groq_for_prediction,
    run_forecast_for_topic,
)

load_dotenv()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
if not XAI_API_KEY:
    raise ValueError("XAI_API_KEY not found")
client = OpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")
MODEL = "grok-3"


# groq_classifier.py (updated)
def classify_intent_with_groq(user_query: str) -> str:
    print("[INFO] Classifying query intent...")

    # Expanded pattern-based classification
    patterns = {
        "PRODUCTIVITY": r"\b(productivity|efficiency|performance|output|hours worked)\b",
        "REVENUE": r"\b(revenue|sales|income|earnings)\b",
        "CUSTOMER_SATISFACTION": r"\b(customer satisfaction|feedback|reviews|ratings)\b",
        "INVENTORY": r"\b(inventory|stock|supply|warehouse)\b",
        "STOCK": r"\b(stock|market|forecast|price|trend|earnings)\b",
        "HYBRID": r"\b(stock|market|forecast)\b.*\b(sales|revenue|inventory|productivity)\b",
        "SIMPLE": r"\b(show|list|get|display)\b",
    }

    query_lower = user_query.lower()
    matched_intents = [
        intent
        for intent, pattern in patterns.items()
        if re.search(pattern, query_lower)
    ]

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
                    ),
                },
                {"role": "user", "content": user_query},
            ],
            temperature=0.2,
        )
        intent = response.choices[0].message.content.strip().upper()
        if intent in patterns:
            print(f"[INFO] Grok-3 classified intent: {intent}")
            return intent
    except Exception as e:
        print(
            f"[ERROR] Grok-3 classification failed: {e}. Using pattern-based fallback."
        )

    # Fallback to pattern-based classification
    if "HYBRID" in matched_intents:
        return "HYBRID"
    elif matched_intents:
        return matched_intents[0].upper()
    return "SIMPLE"

