#!/usr/bin/env python3
# Questo script fornisce un'implementazione equivalente a Spark Job1 usando Hadoop Streaming in Python
# mapper.py
import sys
import csv

reader = csv.reader(sys.stdin)

header = next(reader, None)  # salta intestazione

for row in reader:
    try:
        vin = row[0].strip()
        make = row[42].strip()
        model = row[45].strip()
        price = row[48].strip()
        year = row[65].strip()

        if not (vin and make and model and price and year):
            continue

        price = float(price)
        year = int(year)

        if price <= 0 or not (1900 < year < 2026):
            continue

        if not make[0].isalpha():  # filtra make tipo "42.6 in"
            continue

        print(f"{make}\t{model},{price},{year}")

    except Exception:
        continue
# Gestisce eventuali errori di conversione o di formato
        # Ignora la riga se c'è un errore



