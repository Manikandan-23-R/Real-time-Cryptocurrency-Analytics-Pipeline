import json
import time
from kafka import KafkaProducer

# 🚀 இப்போ உங்க உண்மையான API ஃபைலை மீண்டும் கனெக்ட் பண்றோம்!
from data_fetcher import get_crypto_data 

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "crypto_prices"

def start_producing():
    print("🚀 Kafka Live Producer Pipeline Started...")
    print("📡 Fetching REAL-TIME data from CoinGecko API every 30 seconds...\n")
    
    try:
        while True:
            # 🌐 இப்போ இன்டர்நெட் வழியா ரியல் டேட்டாவை எடுக்கும்
            crypto_events = get_crypto_data()
            
            for event in crypto_events:
                crypto_key = event["coin_id"] 
                
                producer.send(
                    TOPIC_NAME, 
                    key=crypto_key.encode('utf-8'),
                    value=event
                )
                print(f"✅ Sent to Kafka | Key: {crypto_key:<10} | Price: INR {event['current_price']}")
            
            producer.flush() 
            print("⏳ Waiting for 30 seconds...\n")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    start_producing()