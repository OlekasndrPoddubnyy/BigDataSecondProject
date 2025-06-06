#!/usr/bin/env python3
import sys
import csv
import re

reader = csv.DictReader(sys.stdin)

for row in reader:
    try:
        if not all(k in row for k in ["city", "year", "price", "daysonmarket", "description"]):
            continue

        city = row["city"].strip()
        year = int(row["year"])
        price = float(row["price"])
        days = float(row["daysonmarket"])
        description = row["description"].strip().lower()

        if not re.match(r'^[A-Za-z]', city):
            continue
        if price <= 0 or year <= 1900 or year >= 2026:
            continue

        if price < 20000:
            fascia = "low"
        elif price <= 50000:
            fascia = "medium"
        else:
            fascia = "high"

        words = re.findall(r'\b\w+\b', description)
        desc_str = " ".join(words)

        key = f"{city}|{year}|{fascia}"
        value = f"days={days};desc={desc_str}"
        print(f"{key}\t{value}")

    except Exception as e:
        # Rimuovilo se non vuoi vedere output verbose
        print(f"✘ EXCEPTION: {e} su riga: {row}", file=sys.stderr)
        continue