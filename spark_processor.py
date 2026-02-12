import os
import sys
import pyspark

# --- BYPASS HADOOP PERMISSION CHECK ---
# This forces Hadoop to use a Java implementation instead of the missing Windows DLL
os.environ['HADOOP_HOME'] = "C:\\hadoop"  # Point to your hadoop folder if different
os.environ['hadoop.home.dir'] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType


def start_spark_processor():
    # --- 1. AUTO-CONFIGURATION ---
    # Detect the installed Spark version to download the right Kafka connector
    spark_ver = pyspark.__version__
    # Spark 4.x uses Scala 2.13 by default, Spark 3.x uses Scala 2.12
    scala_ver = '2.13' if spark_ver.startswith('4') else '2.12'

    print(f"--- INITIALIZING SPARK {spark_ver} (Scala {scala_ver}) ---")

    # The Maven Coordinate for the Kafka Connector
    maven_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver}:{spark_ver}"

    # Force Spark to download this package
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {maven_package} pyspark-shell'

    # --- 2. START SPARK SESSION ---
    try:
        spark = SparkSession.builder \
            .appName("CrimeAnalyticsRealTime") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")  # Reduce junk logs
    except Exception as e:
        print(f"ERROR STARTING SPARK: {e}")
        print("Tip: If it fails to find the package, try running: pip install pyspark==3.5.0")
        return

    # --- 3. DEFINE SCHEMA (Must match Producer JSON) ---
    schema = StructType() \
        .add("source_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("City", StringType()) \
        .add("latitude", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("Crime Description", StringType())

    # --- 4. READ STREAM FROM KAFKA ---
    print("--- CONNECTING TO KAFKA BROKER ---")
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "crime_reports") \
            .option("startingOffsets", "latest") \
            .load()
    except Exception as e:
        print(f"KAFKA CONNECTION ERROR: {e}")
        return

    # --- 5. PROCESS DATA ---
    # Parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert String Timestamp to Real Timestamp
    formatted_df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))

    # --- 6. CALCULATE ANALYTICS (Rolling Average) ---
    # Count crimes per City in the last 1 minute
    windowed_counts = formatted_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
        window(col("event_time"), "1 minute"),
        col("City")
    ) \
        .agg(count("*").alias("crime_count"))

    # --- 7. APPLY RED ALERT LOGIC ---
    # If > 3 crimes in 1 min -> CRITICAL ALERT
    alert_df = windowed_counts.withColumn(
        "Threat_Level",
        when(col("crime_count") > 3, "🔴 CRITICAL")
        .when(col("crime_count") > 0, "🟡 MODERATE")
        .otherwise("🟢 LOW")
    )

    # --- 8. OUTPUT TO CONSOLE ---
    print("--- STREAMING ANALYTICS STARTED ---")
    query = alert_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    start_spark_processor()