import time
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, lower, split, explode, count, avg, collect_list, struct, map_from_entries
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# Inizializza Spark
spark = SparkSession.builder \
    .appName("UsedCars Job 2 - SparkSQL") \
    .master("local[*]") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Imposta il limite massimo di colonne stampabili nei piani SQL (opzionale)
spark.conf.set("spark.sql.debug.maxToStringFields", 5000)
spark.sparkContext.setLogLevel("ERROR")

# Carica e pulisci
df = spark.read.option("header", True).csv("used_cars_data.csv")
df_clean = df.select("city", "year", "price", "daysonmarket", "description") \
    .dropna(subset=["city", "year", "price", "daysonmarket", "description"]) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("year", col("year").cast("int")) \
    .withColumn("daysonmarket", col("daysonmarket").cast(IntegerType())) \
    .filter((col("price") > 0) & (col("year") > 1900) & (col("year") < 2026)) \
    .filter(regexp_extract(col("city"), r'^[A-Za-z]', 0) != "")

# Fasce di prezzo
df_clean = df_clean.withColumn(
    "price_range",
    when(col("price") > 50000, "high")
    .when((col("price") >= 20000) & (col("price") <= 50000), "medium")
    .otherwise("low")
)

# Crea cartella output
os.makedirs("output_job2_sparksql", exist_ok=True)

# Varianti di test
test_sets = {
    "100k": df_clean.limit(100000),
    "full": df_clean
}

for name, subset in test_sets.items():
    print(f"\n▶️ Inizio test_{name}...")
    start = time.time()

    subset.createOrReplaceTempView("cars")

    # Aggregati per fascia
    aggregated = spark.sql("""
    SELECT
        city,
        year,
        price_range,
        COUNT(*) as number,
        AVG(daysonmarket) as avg_days
    FROM cars
    GROUP BY city, year, price_range
    """)

    # Parole più frequenti
    words = subset \
        .withColumn("word", explode(split(lower(col("description")), "\\W+"))) \
        .filter(col("word") != "")

    word_count = words.groupBy("city", "year", "price_range", "word").count()

    w = Window.partitionBy("city", "year", "price_range").orderBy(col("count").desc())
    top3 = word_count.withColumn("rank", count("*").over(w)) \
        .filter(col("rank") <= 3) \
        .groupBy("city", "year", "price_range") \
        .agg(collect_list("word").alias("description"))

    final = aggregated.join(top3, on=["city", "year", "price_range"], how="left")

    structured = final.groupBy("city", "year").agg(
        collect_list(struct(
            col("price_range"),
            struct(
                col("number").alias("number"),
                col("avg_days").alias("daysonmarket"),
                col("description").alias("description")
            ).alias("data")
        )).alias("fasce")
    )

    output = structured.groupBy("city").agg(
        collect_list(
            struct(
                col("year"),
                map_from_entries("fasce").alias("fasce")
            )
        ).alias("years")
    )

    # Salva il risultato
    data = output.toJSON().collect()
    with open(f"output_job2_sparksql/job2_{name}.json", "w") as f:
        f.write("[\n" + ",\n".join(data) + "\n]")

    duration = round(time.time() - start, 2)
    print(f"✅ test_{name} completato in {duration} secondi.")

    # Pulisce la cache per evitare che il test successivo venga influenzato
    spark.catalog.clearCache()
