import csv
import json
import time
import re
from io import StringIO
from pyspark import SparkContext

sc = SparkContext(master="local[4]", appName="UsedCars Job 2 - Spark Core")
sc.setLogLevel("ERROR")

file_path = "used_cars_data.csv"
raw = sc.textFile(file_path)
header = raw.first()
fields = next(csv.reader(StringIO(header)))
indices = {field: i for i, field in enumerate(fields)}

def parse_line(line):
    try:
        reader = csv.reader(StringIO(line))
        parts = next(reader)
        city = parts[indices["city"]]
        year = int(parts[indices["year"]])
        price = float(parts[indices["price"]])
        days = int(parts[indices["daysonmarket"]])
        description = parts[indices["description"]].lower()

        if not city or not city[0].isalpha():
            return None
        if not (1900 < year < 2026) or price <= 0 or days < 0:
            return None

        if price > 50000:
            fascia = "high"
        elif price >= 20000:
            fascia = "medium"
        else:
            fascia = "low"

        words = re.findall(r"\b\w+\b", description)
        return ((city, year, fascia), (1, days, words))
    except:
        return None

def top_words(words_list):
    from collections import Counter
    flat = [w for sublist in words_list for w in sublist if w]
    return [w for w, _ in Counter(flat).most_common(3)]

def build_stats(values):
    total_count = sum(v[0] for v in values)
    total_days = sum(v[1] for v in values)
    all_words = [v[2] for v in values]
    return {
        "number": total_count,
        "daysonmarket": round(total_days / total_count, 2),
        "description": top_words(all_words)
    }

# Test set
subsets = {
    "100k": raw.zipWithIndex().filter(lambda x: x[1] > 0 and x[1] <= 100000).map(lambda x: x[0]),
    "500k": raw.zipWithIndex().filter(lambda x: x[1] > 0 and x[1] <= 500000).map(lambda x: x[0]),
    "1M": raw.zipWithIndex().filter(lambda x: x[1] > 0 and x[1] <= 1000000).map(lambda x: x[0]),
    "full": raw.filter(lambda r: r != header)
}

for name, data in subsets.items():
    print(f"\n▶️ Inizio test_{name}...")
    start = time.time()

    parsed = data.map(parse_line).filter(lambda x: x is not None)

    # Aggrega per (city, year, fascia)
    grouped = parsed.groupByKey().mapValues(build_stats)

    # Ristruttura in (city, year) → {fascia → stats}
    mapped = grouped.map(lambda x: ((x[0][0], x[0][1]), [(x[0][2], x[1])]))
    combined = mapped.reduceByKey(lambda a, b: a + b)
    structured = combined.map(lambda x: {
        "city": x[0][0],
        "year": x[0][1],
        "fasce": {fascia: stats for fascia, stats in x[1]}
    })

    # Riorganizza per città
    grouped_by_city = structured.map(lambda d: (d["city"], [{
        "year": d["year"],
        "fasce": d["fasce"]
    }])).reduceByKey(lambda a, b: a + b).map(lambda x: {
        "city": x[0],
        "years": x[1]
    })

    # Salva
    output = grouped_by_city.collect()
    with open(f"output_job2_sparkcore/job2_{name}.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ test_{name} completato in {round(time.time() - start, 2)} secondi.")

sc.stop()
