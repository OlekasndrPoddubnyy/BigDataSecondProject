#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

# Dati raccolti: brand -> model -> lista prezzi e anni
data = defaultdict(lambda: defaultdict(lambda: {
    "quantity": 0,
    "prices": [],
    "years": set()
}))

for line in sys.stdin:
    try:
        key, value = line.strip().split('\t')
        model, price, year = value.strip().split(',')

        price = float(price)
        year = int(year)

        model = model.strip()
        brand = key.strip()

        model_data = data[brand][model]
        model_data["quantity"] += 1
        model_data["prices"].append(price)
        model_data["years"].add(year)

    except Exception:
        continue  # ignora righe malformate

# Costruisci output finale come JSON
result = []
for brand, models in data.items():
    brand_models = []
    for model, info in models.items():
        prices = info["prices"]
        brand_models.append({
            "name": model,
            "data": {
                "quantity": info["quantity"],
                "min_price": min(prices),
                "max_price": max(prices),
                "avg_price": round(sum(prices)/len(prices), 2),
                "years": sorted(info["years"])
            }
        })
    result.append((brand, brand_models))

# Ordina per numero di modelli e prendi i primi 10 brand
top_10 = sorted(result, key=lambda x: len(x[1]), reverse=True)[:10]

output = [{"brand": brand, "models": models} for brand, models in top_10]
print(json.dumps(output, indent=4))