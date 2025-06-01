# ------------------------------------------------------------
# generate_seed_yaml.py
#
# Reads the CSV header for `rent_roll_assumptions.csv`
# and writes `models/rent_roll_assumptions.yml` (with not_null tests)
# ------------------------------------------------------------
import csv
import os

# ── CONFIGURE THESE TWO CONSTANTS ───────────────────────────
SEED_NAME     = "rent_roll_assumptions"    # (no “.csv” extension here)
CSV_PATH      = f"seeds/{SEED_NAME}.csv"
YAML_PATH     = f"models/{SEED_NAME}.yml"
DBT_SCHEMA_VER = 2    # use “3” if you’re on a newer dbt that expects version: 3
# ──────────────────────────────────────────────────────────

# 1️⃣ Read the first (header) row from the CSV
with open(CSV_PATH, newline="") as csvfile:
    reader  = csv.reader(csvfile)
    headers = next(reader)

# 2️⃣ Build a YAML list of lines
lines = []
lines.append(f"version: {DBT_SCHEMA_VER}")
lines.append("")
lines.append("seeds:")
lines.append(f"  - name: {SEED_NAME}")
lines.append(f"    description: \"(auto-generated seed schema for {SEED_NAME}.csv)\"")
lines.append("    columns:")

for col in headers:
    lines.append(f"      - name: {col}")
    lines.append("        tests: [ not_null ]")
    lines.append("")

# 3️⃣ Ensure the folder exists, then write out the YAML
os.makedirs(os.path.dirname(YAML_PATH), exist_ok=True)
with open(YAML_PATH, "w") as f:
    f.write("\n".join(lines))

print(f"Wrote seed schema to {YAML_PATH}")
