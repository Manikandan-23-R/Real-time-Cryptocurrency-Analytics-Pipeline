import mysql.connector
import requests
import json
import time

# உன் MySQL Details
MYSQL = {
    "host":     "localhost",
    "user":     "root",
    "password": "mani23",
    "database": "crypto_warehouse"
}

# உன் Push URL இங்க போடு
PUSH_URL = "https://api.powerbi.com/beta/406a6e9a-2c6d-4cd4-b5fc-299c924534e7/datasets/b5084feb-088c-40f1-837c-6b2974cdccd6/rows?redirectedFromSignup=1&ScenarioId=Signup&redirectedWaitSimple=1&experience=power-bi&key=d6kDybpUFqs5A5muE2natrwIrgOLeIwRINKMzGB9wJnRWCsE4xWm3T4x46JHac57IZBPHAZfMcz7mT2%2BeaeaHQ%3D%3D"

def get_latest_prices():
    conn = mysql.connector.connect(**MYSQL)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            dc.name              AS coin_name,
            dc.symbol            AS symbol,
            fp.current_price,
            fp.market_cap,
            fp.volume,
            fp.price_change_1h,
            fp.price_change_24h,
            fp.price_change_7d,
            dm.trend,
            dm.volatility_level,
            fp.recorded_at
        FROM fact_prices fp
        JOIN dim_coin   dc ON fp.coin_key   = dc.coin_key
        JOIN dim_market dm ON fp.market_key = dm.market_key
        ORDER BY fp.recorded_at DESC
        LIMIT 20
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

def push_to_powerbi(rows):
    payload = []
    for row in rows:
        payload.append({
            "coin_name":        row["coin_name"],
            "symbol":           row["symbol"],
            "current_price":    float(row["current_price"]),
            "market_cap":       int(row["market_cap"]),
            "volume":           int(row["volume"]),
            "price_change_1h":  float(row["price_change_1h"] or 0),
            "price_change_24h": float(row["price_change_24h"] or 0),
            "price_change_7d":  float(row["price_change_7d"] or 0),
            "trend":            row["trend"],
            "volatility_level": row["volatility_level"],
            "recorded_at":      str(row["recorded_at"])
        })

    response = requests.post(
        PUSH_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    print(f"Pushed {len(payload)} rows — Status: {response.status_code}")

while True:
    rows = get_latest_prices()
    push_to_powerbi(rows)
    time.sleep(10)