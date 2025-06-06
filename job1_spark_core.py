from pyspark import SparkConf, SparkContext
import json, time, os, csv
from io import StringIO

def parse_line(line):
    try:
        row = next(csv.reader(StringIO(line)))
        vin = row[0].strip()
        make = row[42].strip()
        model = row[45].strip()
        price = float(row[49])
        year = int(row[65])
        if price > 0 and 1900 < year < 2026 and vin and make and model and make[0].isalpha():
            return ((make, model), (vin, price, year))
    except:
        return None

def create_combiner(value):
    vin, price, year = value
    return ({vin}, [price], {year})

def merge_value(acc, value):
    vins, prices, years = acc
    vin, price, year = value
    vins.add(vin)
    prices.append(price)
    years.add(year)
    return (vins, prices, years)

def merge_combiners(a, b):
    return (a[0] | b[0], a[1] + b[1], a[2] | b[2])

def build_stats(acc):
    vins, prices, years = acc
    return {
        "quantity": len(vins),
        "min_price": min(prices),
        "max_price": max(prices),
        "avg_price": round(sum(prices) / len(prices), 2),
        "years": sorted(years)
    }

conf = SparkConf().setAppName("UsedCars Job1 Spark Core") \
                  .setMaster("local[4]") \
                  .set("spark.driver.memory", "20g") \
                  .set("spark.driver.maxResultSize", "8g")
sc = SparkContext(conf=conf)

lines = sc.textFile("used_cars_data.csv")
header = lines.first()
data = lines.filter(lambda row: row != header)
total_records = data.count()

limits = {"100k": 100000, "500k": 500000, "1M": 1000000, "full": None}
os.makedirs("output_job_sparkcore", exist_ok=True)

for name, limit in limits.items():
    print(f"\n▶️ Inizio test_{name}...")
    start = time.time()

    subset = data if limit is None else data.sample(False, limit / total_records, seed=42)

    parsed = subset.map(parse_line).filter(lambda x: x is not None)

    aggregated = parsed.combineByKey(
        create_combiner,
        merge_value,
        merge_combiners
    ).mapValues(build_stats)

    brand_models = aggregated.map(lambda x: (x[0][0], {"name": x[0][1], "data": x[1]})) \
                             .groupByKey() \
                             .mapValues(list) \
                             .cache()

    top_10 = brand_models.mapValues(len).takeOrdered(10, key=lambda x: -x[1])
    top_10_keys = set(k for k, _ in top_10)

    structured = brand_models.filter(lambda x: x[0] in top_10_keys) \
                             .map(lambda x: {"brand": x[0], "models": x[1]}) \
                             .collect()

    with open(f"output_job_sparkcore/{name}.json", "w") as f:
        json.dump(structured, f, indent=4)

    brand_models.unpersist()
    duration = round(time.time() - start, 2)
    print(f"✅ test_{name} completato in {duration} secondi.")

sc.stop()