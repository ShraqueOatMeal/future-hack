# industry_map.py - Maps keywords to stock symbols

# Industry keyword mapping
industry_keywords = {
    "technology": ["AAPL", "MSFT", "GOOGL", "META", "NVDA", "TSLA"],
    "semiconductors": ["NVDA", "AMD", "INTC", "TSM", "QCOM"],
    "consumer_electronics": ["AAPL", "SONY", "SSNLF", "LG"],
    "software": ["MSFT", "GOOGL", "ORCL", "ADBE", "CRM"],
    "mobile": ["AAPL", "GOOGL", "QCOM", "SSNLF"],
    "gaming": ["NVDA", "AMD", "SONY", "ATVI", "EA"],
    "cloud": ["AMZN", "MSFT", "GOOGL", "META"],
    "ai": ["NVDA", "GOOGL", "MSFT", "META", "AMD"],
    "automotive": ["TSLA", "F", "GM", "NIO", "RIVN"],
    "electric_vehicles": ["TSLA", "NIO", "RIVN", "LCID", "XPEV"]
}

# Symbol lookup - maps company names and keywords to industries
symbol_lookup = {
    # Apple
    "apple": "consumer_electronics",
    "aapl": "consumer_electronics", 
    "iphone": "consumer_electronics",
    "macbook": "consumer_electronics",
    "ipad": "consumer_electronics",
    "airpods": "consumer_electronics",
    "mac": "consumer_electronics",
    
    # Microsoft
    "microsoft": "software",
    "msft": "software",
    "surface": "software",
    "windows": "software",
    "office": "software",
    "azure": "cloud",
    
    # Google
    "google": "software",
    "googl": "software",
    "alphabet": "software",
    "pixel": "mobile",
    "nest": "consumer_electronics",
    "android": "mobile",
    
    # NVIDIA
    "nvidia": "semiconductors",
    "nvda": "semiconductors",
    "rtx": "semiconductors",
    "geforce": "gaming",
    "gpu": "semiconductors",
    "graphics": "semiconductors",
    
    # AMD
    "amd": "semiconductors",
    "ryzen": "semiconductors",
    "radeon": "semiconductors",
    
    # Intel
    "intel": "semiconductors",
    "intc": "semiconductors",
    "core": "semiconductors",
    "processor": "semiconductors",
    "cpu": "semiconductors",
    
    # Samsung
    "samsung": "consumer_electronics",
    "galaxy": "consumer_electronics",
    "ssnlf": "consumer_electronics",
    
    # Sony
    "sony": "consumer_electronics",
    "playstation": "gaming",
    "wh-1000xm": "consumer_electronics",
    "headphones": "consumer_electronics",
    
    # Other tech brands
    "dell": "technology",
    "hp": "technology",
    "lenovo": "technology",
    "asus": "technology",
    "razer": "gaming",
    "logitech": "technology",
    "corsair": "gaming",
    "jbl": "consumer_electronics",
    "beats": "consumer_electronics",
    "anker": "consumer_electronics",
    "belkin": "consumer_electronics",
    "oneplus": "mobile",
    "marshall": "consumer_electronics",
    
    # Categories
    "laptop": "technology",
    "smartphone": "mobile",
    "tablet": "consumer_electronics",
    "audio": "consumer_electronics",
    "gaming": "gaming",
    "cooling": "technology",
    "storage": "technology",
    "monitor": "technology",
    "keyboard": "technology",
    "mouse": "technology",
    "peripheral": "technology",
    "wearable": "consumer_electronics",
    
    # Tesla and EV
    "tesla": "electric_vehicles",
    "tsla": "electric_vehicles",
    "electric": "electric_vehicles",
    "ev": "electric_vehicles",
    "vehicle": "automotive",
    "car": "automotive",
    
    # AI and emerging tech
    "artificial intelligence": "ai",
    "machine learning": "ai",
    "ai": "ai",
    "chatgpt": "ai",
    "openai": "ai"
}

# Additional symbol mappings for direct lookups
direct_symbols = {
    "AAPL": "AAPL",
    "MSFT": "MSFT", 
    "GOOGL": "GOOGL",
    "NVDA": "NVDA",
    "AMD": "AMD",
    "INTC": "INTC",
    "TSLA": "TSLA",
    "META": "META",
    "AMZN": "AMZN",
    "SONY": "SONY",
    "SSNLF": "SSNLF"  # Samsung
}