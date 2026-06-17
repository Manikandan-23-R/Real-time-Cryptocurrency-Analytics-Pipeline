import json
import requests
from datetime import datetime, UTC

# =========================
# CONFIG
# =========================

URL = "https://api.coingecko.com/api/v3/coins/markets"

HEADERS = {
    "x-cg-demo-api-key": "CG-moQAS7yyLsaznvmcn3tz6er4"
}

PARAMS = {
    "vs_currency": "inr",
    "ids": "bitcoin,ethereum,solana",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "1h,24h,7d"
}


# =========================
# EXTRACT
# =========================

def extract():
    response = requests.get(
        URL,
        params=PARAMS,
        headers=HEADERS,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


# =========================
# TRANSFORM
# =========================

def transform(record):
    return {
        "coin_id": record.get("id"),
        "symbol": record.get("symbol"),
        "name": record.get("name"),

        "current_price": record.get("current_price"),
        "market_cap": record.get("market_cap"),
        "volume": record.get("total_volume"),

        "price_change_1h":
            record.get("price_change_percentage_1h_in_currency"),

        "price_change_24h":
            record.get("price_change_percentage_24h_in_currency"),

        "price_change_7d":
            record.get("price_change_percentage_7d_in_currency"),

        "recorded_at":
            datetime.now(UTC).isoformat()
    }


# =========================
# MAIN
# =========================

def main():

    raw_data = extract()

    events = [transform(coin) for coin in raw_data]

    print("\nTRANSFORMED DATA\n")

    for event in events:
        print(json.dumps(event, indent=4))
        print("-" * 50)

def get_crypto_data():
    raw_data=extract()
    events=[transform(coin) for coin in raw_data]
    return events

if __name__=="__main__":
    main()











