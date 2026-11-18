import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

STREAM_DIR = "data/streaming_inbox"
os.makedirs(STREAM_DIR, exist_ok=True)
os.makedirs("delta-lake/bronze", exist_ok=True)

DELTA_BRONZE_PATH = os.path.abspath("delta-lake/bronze/wildfires_stream")
CHECKPOINT_PATH = os.path.abspath("delta-lake/checkpoints/bronze_wildfires")

def create_spark_session():
    """Initializes PySpark with Delta Lake and Apache Sedona using correct extension class paths."""
    return (
        SparkSession.builder
        .appName("AegisStream-FileIngestionEngine")
        .master("local[*]")
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.sql.SedonaSqlExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Modern Java module export for Hadoop Subject compatibility
        .config("spark.driver.extraJavaOptions", "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("PySpark session successfully initialized with Sedona & Delta Lake!")

    # Define Schema matching our JSON event files
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("brightness_k", DoubleType(), True),
        StructField("frp_mw", DoubleType(), True),
        StructField("confidence", StringType(), True),
        StructField("satellite", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_dir_deg", DoubleType(), True),
    ])

    print(f"Watching directory '{STREAM_DIR}' for incoming streaming files...")
    
    # Read streaming files as they appear in the folder
    streaming_df = (
        spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(STREAM_DIR)
    )

    # Add ingestion timestamp
    bronze_stream_df = (
        streaming_df
        .withColumn("ingested_at", current_timestamp())
    )

    print("Starting Spark Structured Streaming -> Writing to local Delta Lake (Bronze)...")
    query = (
        bronze_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(DELTA_BRONZE_PATH)
    )

    print("Streaming pipeline is live! Drop or produce files into streaming_inbox...")
    query.awaitTermination()

if __name__ == "__main__":
    main()