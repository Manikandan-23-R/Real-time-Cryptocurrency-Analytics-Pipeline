import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 11, 5),
    auto_offset_reset='earliest',
    group_id="my_crypto_group_unique_1",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

partitions = consumer.partitions_for_topic('crypto_prices')
print(f"📊 இந்த டாபிக்ல மொத்தம் {len(partitions)} பார்ட்டிஷன் இருக்குது!")
print(f"📦 Partition List: {list(partitions)}")
print("-" * 70)
print("🎧 Listening for Crypto data... Watch how Keys match Partitions!")
print("-" * 70)

try:
    for message in consumer:
        # 🛠️ கஃப்காவுக்குள் போன Key-ஐத் திரும்ப எடுத்து ஸ்ட்ரிங்காக மாற்றுகிறோம்
        message_key = message.key.decode('utf-8') if message.key else "No Key"
        
        # இப்போ பிரிண்ட் பண்ணும்போது [Partition], [Offset] மற்றும் [Key] மூன்றும் காட்டும்
        print(f"📥 [Partition: {message.partition}] | [Offset: {message.offset}] | [Key: {message_key}]")
        
        coin_data = message.value
        print(f"🪙 Coin: {coin_data.get('name')} | Price: INR {coin_data.get('current_price')}")
        print("-" * 70)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped by user.")
finally:
    consumer.close()