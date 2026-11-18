import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from sedona.register import SedonaRegistrator

SILVER_PATH = os.path.abspath("delta-lake/silver/wildfires_spatial")
GOLD_PATH = os.path.abspath("delta-lake/gold/evacuation_alerts")

def create_spark_session():
    return (
        SparkSession.builder
        .appName("AegisStream-GoldEvacuationIntelligence")
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
    SedonaRegistrator.registerAll(spark)
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session initialized for Gold Evacuation Intelligence.")

    # 1. Read the Silver Spatial Table as a batch DataFrame
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    # 2. Apply risk evaluation logic based on brightness and wind speed
    enriched_risk_df = (
        silver_df
        .withColumn(
            "risk_level",
            expr("CASE WHEN brightness_k > 345 AND wind_speed_kmh > 2.0 THEN 'CRITICAL_EVACUATION_WARNING' ELSE 'MONITORING' END")
        )
    )

    print("\nSample Gold Intelligence Records:")
    enriched_risk_df.select("event_id", "geometry", "brightness_k", "wind_speed_kmh", "risk_level").show(5, truncate=False)

    # 3. Write out to Gold Delta Table storage using standard batch write
    print(f"Writing Gold Evacuation Intelligence table to: {GOLD_PATH}")
    (
        enriched_risk_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )
    print("Gold layer successfully processed and saved!")

if __name__ == "__main__":
    main()
