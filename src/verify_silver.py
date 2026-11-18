import os
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator

def main():
    spark = (
        SparkSession.builder
        .appName("AegisStream-VerifySilver")
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
    
    SedonaRegistrator.registerAll(spark)
    spark.sparkContext.setLogLevel("WARN")

    silver_path = os.path.abspath("delta-lake/silver/wildfires_spatial")
    print(f"Reading Silver Delta table from: {silver_path}")
    
    # Read the batch/snapshot of the Silver Delta table
    df = spark.read.format("delta").load(silver_path)
    
    print(f"\nTotal records in Silver Spatial Table: {df.count()}")
    
    print("\nSample Wildfire Events with Parsed Sedona Geometries:")
    df.select("event_id", "latitude", "longitude", "geometry", "brightness_k", "wind_speed_kmh").show(5, truncate=False)

if __name__ == "__main__":
    main()
