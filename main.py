import os
import socket
import csv
import time
import datetime
from functools import wraps
from collections import defaultdict

# ← Ajouté : configuration du parallélisme entre conteneurs
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "1"))
_host = socket.gethostname()
try:
    # Si hostname = projet_worker_3 → worker_id = 2
    WORKER_ID = int(_host.rsplit("_", 1)[-1]) - 1
except ValueError:
    # fallback sur une variable d'env si nécessaire
    WORKER_ID = int(os.getenv("WORKER_ID", "0"))

class ValidationError(Exception):
    """Raised when a CSV row fails validation."""
    pass

# Performance decorator
def timed(func):
    """Decorator to measure execution time of functions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"[PERF] {func.__name__} took {end - start:.4f}s")
        return result
    return wrapper

# 1. CSV Parsing and Validation
def parse_csv(file_path, delimiter=','):
    """
    Generator yielding validated rows from a CSV file, skipping blank lines.
    :param file_path: Path to the CSV file.
    :param delimiter: Field delimiter (default ',').
    :yields: dict representing each validated row.
    """
    with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
        lines = (line for line in csvfile if line.strip())
        reader = csv.DictReader(lines, delimiter=delimiter)
        # Normalise header keys
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
        for line_num, row in enumerate(reader, start=2):
            try:
                yield validate_row({k.strip(): v for k, v in row.items()})
            except ValidationError as ve:
                print(f"[ERROR] Line {line_num}: {ve}")

def validate_row(row):
    """Validate and cast fields per schema."""
    for key in ('transaction_id', 'date', 'ville', 'modele'):
        if not row.get(key) or not row[key].strip():
            raise ValidationError(f"Missing '{key}'")
    t = row.get('type')
    if t not in ('vente', 'location'):
        raise ValidationError("Invalid 'type', must be 'vente' or 'location'")
    row['type'] = t
    try:
        price = float(row.get('prix', ''))
        if price < 0:
            raise ValidationError("'prix' must be non-negative")
        row['prix'] = price
    except ValueError:
        raise ValidationError("Invalid 'prix'")
    if t == 'location':
        val = row.get('duree_location_mois')
        if not val or not val.strip():
            raise ValidationError("Missing 'duree_location_mois' for location")
        try:
            d = int(val)
            if d < 1:
                raise ValidationError("'duree_location_mois' must be >= 1")
            row['duree_location_mois'] = d
        except ValueError:
            raise ValidationError("Invalid 'duree_location_mois'")
    else:
        row['duree_location_mois'] = None
    if 'cost' in row and row.get('cost') and row['cost'].strip():
        try:
            row['cost'] = float(row['cost'])
        except ValueError:
            raise ValidationError("Invalid 'cost'")
    return row

# 2. Calculation Algorithms
@timed
def calculate_total_revenue(rows):
    return sum(r['prix'] for r in rows)

@timed
def revenue_by_modele(rows):
    rev = defaultdict(float)
    for r in rows:
        rev[r['modele']] += r['prix']
    return dict(rev)

@timed
def revenue_by_ville(rows):
    rev = defaultdict(float)
    for r in rows:
        rev[r['ville']] += r['prix']
    return dict(rev)

@timed
def average_duration_per_modele(rows):
    total, count = defaultdict(int), defaultdict(int)
    for r in rows:
        if r['type'] == 'location':
            total[r['modele']] += r['duree_location_mois']
            count[r['modele']] += 1
    return {m: total[m]/count[m] for m in total}

@timed
def top_n(d, n=5):
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

# 3. Additional analyses
@timed
def top_models_by_city(rows, n=5):
    counts = defaultdict(lambda: defaultdict(int))
    for r in rows:
        counts[r['ville']][r['modele']] += 1
    return {city: top_n(mods, n) for city, mods in counts.items()}

@timed
def monthly_revenue(rows):
    rev = defaultdict(float)
    for r in rows:
        dt = datetime.datetime.strptime(r['date'], "%Y-%m-%d")
        key = dt.strftime("%Y-%m")
        rev[key] += r['prix']
    return dict(sorted(rev.items()))

@timed
def average_rotation(rows):
    durations = [r['duree_location_mois'] for r in rows if r['type']=='location']
    return sum(durations)/len(durations) if durations else 0

@timed
def average_margin_by_type_and_city(rows):
    sums = defaultdict(lambda: defaultdict(float))
    counts = defaultdict(lambda: defaultdict(int))
    for r in rows:
        cost = r.get('cost')
        if cost is None:
            continue
        margin = r['prix'] - cost
        sums[r['type']][r['ville']] += margin
        counts[r['type']][r['ville']] += 1
    return {t: {city: sums[t][city]/counts[t][city]
                for city in sums[t]} for t in sums}

@timed
def predict_trend(rows):
    rev = monthly_revenue(rows)
    months, vals = list(rev.keys()), list(rev.values())
    n = len(vals)
    xs = list(range(n))
    sx, sy = sum(xs), sum(vals)
    sxy = sum(x*y for x,y in zip(xs, vals))
    sx2 = sum(x*x for x in xs)
    denom = n*sx2 - sx*sx
    slope = (n*sxy - sx*sy)/denom if denom else 0
    intercept = (sy - slope*sx)/n if n else 0
    last = datetime.datetime.strptime(months[-1], "%Y-%m")
    year = last.year + (last.month // 12)
    month = last.month % 12 + 1
    next_key = f"{year}-{month:02d}"
    predicted = slope*n + intercept
    return next_key, predicted

# 4. Main pipeline
@timed
def main_pipeline(file_path):
    # ← Modifié : on ne garde que la part du CSV qui nous revient
    all_rows = parse_csv(file_path)
    rows = [r for idx, r in enumerate(all_rows) if idx % NUM_WORKERS == WORKER_ID]
    print(f"[Worker {WORKER_ID+1}/{NUM_WORKERS}] Processing {len(rows)} rows\n")

    total = calculate_total_revenue(rows)
    print(f"Total revenue: {total:.2f}\n")

    by_modele = revenue_by_modele(rows)
    print("Top 5 modèles by revenue:")
    for m, rev in top_n(by_modele):
        print(f"  {m}: {rev:.2f}")
    print()

    by_ville = revenue_by_ville(rows)
    print("Revenue by city:")
    for city, rev in by_ville.items():
        print(f"  {city}: {rev:.2f}")
    print()

    avg_dur = average_duration_per_modele(rows)
    print("Average rental duration per model (mois):")
    for m, d in avg_dur.items():
        print(f"  {m}: {d:.1f}")
    print()

    tops = top_models_by_city(rows)
    print("Top 5 modèles par ville:")
    for city, lst in tops.items():
        print(f"  {city}: {lst}")
    print()

    m_rev = monthly_revenue(rows)
    print("Monthly revenue:")
    for mon, rev in m_rev.items():
        print(f"  {mon}: {rev:.2f}")
    print()

    avg_rot = average_rotation(rows)
    print(f"Overall avg rental duration: {avg_rot:.1f} mois\n")

    margins = average_margin_by_type_and_city(rows)
    if margins:
        print("Average margin by type and city:")
        for t, cities in margins.items():
            for city, m in cities.items():
                print(f"  {t} - {city}: {m:.2f}")
        print()
    else:
        print("No 'cost' field found; skip margin calculations.\n")

    next_key, pred = predict_trend(rows)
    print(f"Trend prediction for {next_key}: {pred:.2f}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Student2 project pipeline")
    parser.add_argument("file", help="Path to CSV input file")
    args = parser.parse_args()
    main_pipeline(args.file)
