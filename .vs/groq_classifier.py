import os
import requests
from dotenv import load_dotenv

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "llama3-70b-8192"

def classify_intent_with_groq(user_input: str) -> str:
    """
    Uses Groq to classify user input into one of these intents:
    - DATABASE: asking about internal company records (sales, products, inventory)
    - STOCK: asking about public stock performance (prices, predictions)
    - NEWS: asking about trends or global financial news
    - HYBRID: asking a question that relates both internal data and stock/market impact
    - UNKNOWN: unclear intent
    """

    system_prompt = (
        "You are an intent classifier. Categorize the user query as one of:\n\n"
        "- DATABASE: Questions about company data (products, sales, workers, etc)\n"
        "- STOCK: Questions about public companies or stock symbols (e.g. TSLA, NVDA)\n"
        "- NEWS: Questions about market trends, headlines, or industry events\n"
        "- HYBRID: Questions that involve both company data and external stock/market impacts\n"
        "- UNKNOWN: If the intent is unclear\n\n"
        "Return ONLY one word: DATABASE, STOCK, NEWS, HYBRID, or UNKNOWN."
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input}
    ]

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    data = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0.1
    }

    try:
        response = requests.post(GROQ_API_URL, headers=headers, json=data)
        result = response.json()
        intent = result["choices"][0]["message"]["content"].strip().upper()

        if intent in {"DATABASE", "STOCK", "NEWS", "HYBRID"}:
            return intent
        return "UNKNOWN"

    except Exception as e:
        print("[ERROR] Intent classification failed:", e)
        return "UNKNOWN"
