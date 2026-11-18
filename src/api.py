import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator

app = FastAPI(
    title="Aegis Stream API",
    description="Zero-cost local Apache Spark wildfire & evacuation intelligence engine for California.",
    version="1.0.0"
)

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GOLD_PATH = os.path.abspath("delta-lake/gold/evacuation_alerts")

spark = None

@app.on_event("startup")
def startup_event():
    global spark
    print("Initializing Spark & Sedona session for FastAPI backend...")
    spark = (
        SparkSession.builder
        .appName("AegisStream-FastAPI-Backend")
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
    print("FastAPI Spark backend ready!")

@app.get("/api/status")
def read_root():
    return {
        "project": "Aegis Stream",
        "status": "online",
        "endpoints": {
            "all_alerts": "/api/alerts",
            "risk_statistics": "/api/stats",
            "interactive_docs": "/docs"
        }
    }

@app.get("/api/alerts")
def get_evacuation_alerts(risk_level: str = None):
    """Retrieve wildfire evacuation alerts, optionally filtered by risk level."""
    if not os.path.exists(GOLD_PATH):
        raise HTTPException(status_code=404, detail="Gold Delta table not found.")
    
    df = spark.read.format("delta").load(GOLD_PATH)
    if risk_level:
        df = df.filter(df.risk_level == risk_level.upper())
        
    pdf = df.select("event_id", "latitude", "longitude", "brightness_k", "wind_speed_kmh", "risk_level").toPandas()
    return {
        "total_records": len(pdf),
        "data": pdf.to_dict(orient="records")
    }

@app.get("/api/stats")
def get_risk_statistics():
    """Retrieve summary counts of events grouped by risk level."""
    if not os.path.exists(GOLD_PATH):
        raise HTTPException(status_code=404, detail="Gold Delta table not found.")
    
    df = spark.read.format("delta").load(GOLD_PATH)
    stats_df = df.groupBy("risk_level").count().toPandas()
    
    return {
        "risk_distribution": stats_df.to_dict(orient="records")
    }

# Mount static files AFTER API routes so they don't override /api paths
app.mount("/", StaticFiles(directory="static", html=True), name="static")
