import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator  # Import Sedona registrator

DELTA_BRONZE_PATH = os.path.abspath("delta-lake/bronze/wildfires_stream")
DELTA_SILVER_PATH = os.path.abspath("delta-lake/silver/wildfires_spatial")
CHECKPOINT_PATH = os.path.abspath("delta-lake/checkpoints/silver_wildfires")

def create_spark_session():
    return (
        SparkSession.builder
        .appName("AegisStream-SilverSpatialLayer")
        .master("local[*]")
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
                "org.datasyslab:geotools-wrapper:1.6.1-28.2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.sql.SedonaSqlExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    
    # Explicitly register Sedona SQL functions and types
    SedonaRegistrator.registerAll(spark)
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session & Sedona spatial functions initialized successfully!")

    # Read the Bronze Delta table as a stream
    bronze_df = (
        spark.readStream
        .format("delta")
        .load(DELTA_BRONZE_PATH)
    )

    # Use Apache Sedona's ST_Point function to convert lat/lon into spatial geometries
    spatial_df = (
        bronze_df
        .withColumn("geometry", expr("ST_Point(CAST(longitude AS Double), CAST(latitude AS Double))"))
    )

    print("Writing Silver Spatial Delta table...")
    query = (
        spatial_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(DELTA_SILVER_PATH)
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
