from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, countDistinct, collect_set
from pyspark.sql.functions import regexp_extract
import json
import time
import os

# Inizializza Spark
spark = SparkSession.builder \
    .appName("UsedCars Job 1 - Cleaned Structured Output") \
    .master("local[8]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Leggi il dataset robustamente
df = spark.read.option("header", True).option("multiLine", True).csv("used_cars_data.csv")

# Cast e filtro righe corrette, includendo 'vin'
df_clean = df.select("vin", "make_name", "model_name", "price", "year") \
    .dropna(subset=["vin", "make_name", "model_name", "price", "year"]) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("year", col("year").cast("int")) \
    .filter(col("price").isNotNull() & col("year").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("year") > 1900) \
    .filter(col("year") < 2026)

    # Mantieni solo make_name che inizia con una lettera (filtra fuori "42.6 in", ecc.)
df_clean = df_clean.filter(regexp_extract(col("make_name"), r'^[A-Za-z]', 0) != "")

#  Test set con limiti diversi
test_sets = {
    "100k": df_clean.limit(100000),
    "500k": df_clean.limit(500000),
    "1M": df_clean.limit(1000000),
    "full": df_clean
}

#  Prepara cartella output
os.makedirs("output_job_sparksql", exist_ok=True)
durations = {}

#  Esegui i job
for name, subset in test_sets.items():
    print(f"\n▶️ Inizio test_{name}...")

    start = time.time()

    grouped = subset.groupBy("make_name", "model_name").agg(
        countDistinct("vin").alias("quantity"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        collect_set("year").alias("years")
    ).collect()

    result = {}
    for row in grouped:
        brand = row["make_name"].strip()
        model = row["model_name"].strip()
        if brand not in result:
            result[brand] = []
        result[brand].append({
            "name": model,
            "data": {
                "quantity": row["quantity"],
                "min_price": row["min_price"],
                "max_price": row["max_price"],
                "avg_price": round(row["avg_price"], 2) if row["avg_price"] else None,
                "years": sorted(row["years"])
            }
        })

    #  Ordina le marche per numero di modelli e prendi le prime 10
    top_10_brands = sorted(result.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    structured = [{"brand": brand, "models": models} for brand, models in top_10_brands]

    #  Salva in JSON
    with open(f"output_job_sparksql/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    duration = round(time.time() - start, 2)
    durations[name] = duration
    print(f" test_{name} completato in {duration} secondi.")

# Ferma Spark
spark.stop()
