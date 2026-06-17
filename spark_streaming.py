import os
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

import spark_config

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import mysql.connector
from datetime import datetime

# =============================================
# MySQL Connection Config
# =============================================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "mani23",  # ← change this!
    "database": "crypto_warehouse"
}

# =============================================
# Helper: Get or Insert dim_coin
# =============================================
def get_coin_key(cursor, coin_id, symbol, name):
    cursor.execute("SELECT coin_key FROM dim_coin WHERE coin_id = %s", (coin_id,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(
        "INSERT INTO dim_coin (coin_id, symbol, name) VALUES (%s, %s, %s)",
        (coin_id, symbol, name)
    )
    return cursor.lastrowid

# =============================================
# Helper: Get or Insert dim_date
# =============================================
def get_date_key(cursor, dt):
    full_date = dt.date()
    cursor.execute("SELECT date_key FROM dim_date WHERE full_date = %s", (full_date,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute("""
        INSERT INTO dim_date (full_date, day, month, year, quarter, day_name, month_name, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        full_date,
        dt.day,
        dt.month,
        dt.year,
        (dt.month - 1) // 3 + 1,
        dt.strftime("%A"),
        dt.strftime("%B"),
        dt.weekday() >= 5
    ))
    return cursor.lastrowid

# =============================================
# Helper: Get or Insert dim_time
# =============================================
def get_time_key(cursor, dt):
    hour = dt.hour
    cursor.execute("SELECT time_key FROM dim_time WHERE hour = %s", (hour,))
    result = cursor.fetchone()
    if result:
        return result[0]
    if 0 <= hour < 12:
        session = "Asia Session"
        am_pm = "AM"
    elif 12 <= hour < 18:
        session = "Europe Session"
        am_pm = "PM"
    else:
        session = "US Session"
        am_pm = "PM"
    cursor.execute("""
        INSERT INTO dim_time (full_time, hour, minute, am_pm, trading_session)
        VALUES (%s, %s, %s, %s, %s)
    """, (dt.strftime("%H:%M:%S"), hour, dt.minute, am_pm, session))
    return cursor.lastrowid

# =============================================
# Helper: Get or Insert dim_market
# =============================================
def get_market_key(cursor, coin_id):
    rank_map = {"bitcoin": 1, "ethereum": 2, "solana": 3}
    rank = rank_map.get(coin_id, 99)
    cursor.execute("SELECT market_key FROM dim_market WHERE market_cap_rank = %s", (rank,))
    result = cursor.fetchone()
    if result:
        return result[0]
    category = "Large Cap" if rank <= 2 else "Mid Cap"
    cursor.execute("""
        INSERT INTO dim_market (market_cap_rank, market_cap_category, volatility_level, trend)
        VALUES (%s, %s, %s, %s)
    """, (rank, category, "Medium", "Bullish"))
    return cursor.lastrowid

# =============================================
# Write Each Batch to MySQL
# =============================================
def write_to_mysql(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"⏭️ Batch {batch_id} — empty, skipping...")
        return

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    rows = batch_df.collect()
    inserted = 0

    for row in rows:
        try:
            dt = datetime.now()

            coin_key   = get_coin_key(cursor, row.coin_id, row.symbol, row.name)
            date_key   = get_date_key(cursor, dt)
            time_key   = get_time_key(cursor, dt)
            market_key = get_market_key(cursor, row.coin_id)

            cursor.execute("""
                INSERT INTO fact_prices (
                    coin_key, date_key, time_key, market_key,
                    current_price, market_cap, volume,
                    price_change_1h, price_change_24h, price_change_7d,
                    recorded_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                coin_key, date_key, time_key, market_key,
                row.current_price,
                row.market_cap,
                row.volume,
                row.price_change_1h,
                row.price_change_24h,
                row.price_change_7d,
                dt
            ))
            inserted += 1

        except Exception as e:
            print(f"❌ Error inserting {row.coin_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Batch {batch_id} — {inserted} rows inserted into MySQL!")

# =============================================
# Main Spark Streaming
# =============================================
def start_spark_analytics():
    print("🚀 Starting Spark Streaming → MySQL Pipeline...")

    spark = SparkSession.builder \
        .appName("CryptoLiveAnalytics") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/temp/spark") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read from Kafka
    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "crypto_prices") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    coin_schema = StructType([
        StructField("coin_id",          StringType(), True),
        StructField("symbol",           StringType(), True),
        StructField("name",             StringType(), True),
        StructField("current_price",    DoubleType(), True),
        StructField("market_cap",       LongType(),   True),
        StructField("volume",           LongType(),   True),
        StructField("price_change_1h",  DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_7d",  DoubleType(), True),
        StructField("recorded_at",      StringType(), True)
    ])

    parsed_df = kafka_raw_df \
        .selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), coin_schema).alias("data")) \
        .select("data.*")

    # Write to MySQL using foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("📡 Listening to Kafka → Writing to MySQL every 30 seconds...")
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_analytics()