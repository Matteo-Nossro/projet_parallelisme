# student2.py
# Module for Student 2: CSV parsing, validation, calculation algorithms, performance measurement, and error handling.
import csv
import time
import datetime
from functools import wraps
from collections import defaultdict

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
    # Use utf-8-sig to handle BOM if present
    with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
        # Skip any blank lines to ensure correct header parsing
        lines = (line for line in csvfile if line.strip())
        reader = csv.DictReader(lines, delimiter=delimiter)
        for line_num, row in enumerate(reader, start=2):
            try:
                yield validate_row(row)
            except ValidationError as ve:
                print(f"[ERROR] Line {line_num}: {ve}")


def validate_row(row):
    """Validate and cast fields per schema."""
    # Normalize keys
    row = {k.strip(): v for k, v in row.items()}
    # Mandatory string fields
    for key in ('transaction_id', 'date', 'ville', 'modele'):
        if not row.get(key) or not row[key].strip():
            raise ValidationError(f"Missing '{key}'")
    # type
    t = row.get('type')
    if t not in ('vente', 'location'):
        raise ValidationError("Invalid 'type', must be 'vente' or 'location'")
    row['type'] = t
    # prix
    try:
        price = float(row.get('prix', ''))
        if price < 0:
            raise ValidationError("'prix' must be non-negative")
        row['prix'] = price
    except ValueError:
        raise ValidationError("Invalid 'prix'")
    # duree_location_mois
    if t == 'location':
        val = row.get('duree_location_mois')
        if not val or not val.strip():
            raise ValidationError("Missing 'duree_location_mois' for location")
        try:
            duration = int(val)
            if duration < 1:
                raise ValidationError("'duree_location_mois' must be >= 1")
            row['duree_location_mois'] = duration
        except ValueError:
            raise ValidationError("Invalid 'duree_location_mois'")
    else:
        row['duree_location_mois'] = None
    # optional cost
    if 'cost' in row and row.get('cost') and row['cost'].strip():
        try:
            row['cost'] = float(row['cost'])
        except ValueError:
            raise ValidationError("Invalid 'cost'")
    return row

# 2. Calculation Algorithms
@timed
def calculate_total_revenue(rows):
    """Total revenue."""
    return sum(r['prix'] for r in rows)

@timed
def revenue_by_modele(rows):
    """Revenue per model."""
    rev = defaultdict(float)
    for r in rows:
        rev[r['modele']] += r['prix']
    return dict(rev)

@timed
def revenue_by_ville(rows):
    """Revenue per city."""
    rev = defaultdict(float)
    for r in rows:
        rev[r['ville']] += r['prix']
    return dict(rev)

@timed
def average_duration_per_modele(rows):
    """Avg rental duration per model."""
    total, count = defaultdict(int), defaultdict(int)
    for r in rows:
        if r['type'] == 'location':
            m = r['modele']
            total[m] += r['duree_location_mois']
            count[m] += 1
    return {m: total[m]/count[m] for m in total}

@timed
def top_n(d, n=5):
    """Top N entries sorted by value desc."""
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

# 3. Additional analyses
@timed
def top_models_by_city(rows, n=5):
    """Top N models by number of transactions in each city."""
    counts = defaultdict(lambda: defaultdict(int))
    for r in rows:
        counts[r['ville']][r['modele']] += 1
    return {city: top_n(mods, n) for city, mods in counts.items()}

@timed
def monthly_revenue(rows):
    """Total revenue per month (YYYY-MM)."""
    rev = defaultdict(float)
    for r in rows:
        dt = datetime.datetime.strptime(r['date'], "%Y-%m-%d")
        key = dt.strftime("%Y-%m")
        rev[key] += r['prix']
    return dict(sorted(rev.items()))

@timed
def average_rotation(rows):
    """Overall avg rental duration."""
    durations = [r['duree_location_mois'] for r in rows if r['type']=='location']
    return sum(durations)/len(durations) if durations else 0

@timed
def average_margin_by_type_and_city(rows):
    """Avg profit margin per transaction type and city."""
    sums = defaultdict(lambda: defaultdict(float))
    counts = defaultdict(lambda: defaultdict(int))
    for r in rows:
        cost = r.get('cost')
        if cost is None:
            continue
        margin = r['prix'] - cost
        sums[r['type']][r['ville']] += margin
        counts[r['type']][r['ville']] += 1
    return {t: {city: sums[t][city]/counts[t][city] for city in sums[t]} for t in sums}

@timed
def predict_trend(rows):
    """Simple linear regression on monthly revenue to predict next month."""
    rev = monthly_revenue(rows)
    months, values = list(rev.keys()), list(rev.values())
    n = len(values)
    x = list(range(n))
    sum_x, sum_y = sum(x), sum(values)
    sum_xy = sum(xi*yi for xi, yi in zip(x, values))
    sum_x2 = sum(xi*xi for xi in x)
    denom = n*sum_x2 - sum_x*sum_x
    slope = (n*sum_xy - sum_x*sum_y)/denom if denom else 0
    intercept = (sum_y - slope*sum_x)/n if n else 0
    # next month key
    last = datetime.datetime.strptime(months[-1], "%Y-%m")
    year = last.year + (last.month // 12)
    month = last.month % 12 + 1
    next_key = f"{year}-{month:02d}"
    predicted = slope*n + intercept
    return next_key, predicted

# 4. Main pipeline
@timed
def main_pipeline(file_path):
    rows = list(parse_csv(file_path))
    print(f"Processed {len(rows)} valid rows\n")

    total = calculate_total_revenue(rows)
    print(f"Total revenue: {total:.2f}\n")

    by_modele = revenue_by_modele(rows)
    print("Top 5 modèles by revenue:")
    for m, r in top_n(by_modele): print(f"  {m}: {r:.2f}")
    print()

    by_ville = revenue_by_ville(rows)
    print("Revenue by city:")
    for city, r in by_ville.items(): print(f"  {city}: {r:.2f}")
    print()

    avg_dur = average_duration_per_modele(rows)
    print("Average rental duration per model (mois):")
    for m, d in avg_dur.items(): print(f"  {m}: {d:.1f}")
    print()

    tops = top_models_by_city(rows)
    print("Top 5 modèles par ville:")
    for city, lst in tops.items(): print(f"  {city}: {lst}")
    print()

    m_rev = monthly_revenue(rows)
    print("Monthly revenue:")
    for mon, r in m_rev.items(): print(f"  {mon}: {r:.2f}")
    print()

    avg_rot = average_rotation(rows)
    print(f"Overall avg rental duration: {avg_rot:.1f} mois\n")

    margins = average_margin_by_type_and_city(rows)
    if margins:
        print("Average margin by type and city:")
        for t, cities in margins.items():
            for city, m in cities.items(): print(f"  {t} - {city}: {m:.2f}")
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
