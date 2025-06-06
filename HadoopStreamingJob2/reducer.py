#!/usr/bin/env python3
import sys
import json
from collections import defaultdict, Counter

# Nested structure: { city: { (year, fascia): {number, total_days, word_counter, count} } }
data = defaultdict(lambda: defaultdict(lambda: {"number": 0, "total_days": 0.0, "word_counter": Counter(), "count": 0}))

def parse_line(line):
    try:
        key, value = line.strip().split("\t")
        city, year, fascia = key.split("|")
        fields = dict(part.split("=", 1) for part in value.strip().split(";") if "=" in part)
        days = float(fields.get("days", 0.0))
        words = fields.get("desc", "").split()
        return city, int(year), fascia, days, words
    except:
        return None

# Read all input and aggregate
for line in sys.stdin:
    parsed = parse_line(line)
    if not parsed:
        continue
    city, year, fascia, days, words = parsed
    key = (year, fascia)
    d = data[city][key]
    d["number"] += 1
    d["total_days"] += days
    d["word_counter"].update(words)
    d["count"] += 1

# Emit final nested JSON
for city, year_data in data.items():
    result = {"city": city, "years": []}
    for (year, fascia), values in sorted(year_data.items()):
        avg_days = values["total_days"] / values["count"] if values["count"] > 0 else 0
        top_words = [w for w, _ in values["word_counter"].most_common(3)]
        # Search if year already exists
        year_entry = next((y for y in result["years"] if y["year"] == year), None)
        if not year_entry:
            year_entry = {"year": year, "fasce": {}}
            result["years"].append(year_entry)
        year_entry["fasce"][fascia] = {
            "number": values["number"],
            "daysonmarket": avg_days,
            "description": top_words
        }
    print(json.dumps(result, ensure_ascii=False))