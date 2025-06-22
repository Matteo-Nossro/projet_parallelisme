import os
import time
import datetime
from functools import wraps
from collections import defaultdict

import pandas as pd
import dask.dataframe as dd
from dask import delayed
from dask.distributed import Client, as_completed

# ----- 1. Validation -----
class ValidationError(Exception):
    """Raised when un CSV row fails validation."""
    pass

def validate_row(row):
    """Valide et cast des champs selon le schéma."""
    for key in ('transaction_id', 'date', 'ville', 'modele'):
        if not row.get(key) or not str(row[key]).strip():
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
        if val is None or str(val).strip() == '':
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

    if 'cost' in row and row.get('cost') not in (None, ''):
        try:
            row['cost'] = float(row['cost'])
        except ValueError:
            raise ValidationError("Invalid 'cost'")
    else:
        row['cost'] = None

    try:
        row['date'] = datetime.datetime.strptime(row['date'], "%Y-%m-%d")
    except ValueError:
        raise ValidationError("Invalid 'date' format, expected YYYY-MM-DD")

    return row

def validate_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Applique validate_row à chaque ligne et ré-ordonne les colonnes."""
    valid = []
    for idx, row in df.iterrows():
        try:
            valid.append(validate_row(row.to_dict()))
        except ValidationError as e:
            print(f"[ERROR] Row {idx}: {e}")

    if not valid:
        # Retourne un DataFrame vide avec les bonnes colonnes
        cols = [
            'transaction_id', 'date', 'ville',
            'modele', 'type', 'prix',
            'duree_location_mois', 'cost'
        ]
        return pd.DataFrame(columns=cols)

    out = pd.DataFrame(valid)
    cols = [
        'transaction_id', 'date', 'ville',
        'modele', 'type', 'prix',
        'duree_location_mois', 'cost'
    ]
    return out[cols]

@delayed
def predict_trend_from_series(monthly_rev: dict):
    """Prédiction de tendance simple."""
    if not monthly_rev:
        return "N/A", 0.0

    months = list(monthly_rev.keys())
    vals = list(monthly_rev.values())
    n = len(vals)

    if n < 2:
        return months[-1] if months else "N/A", vals[-1] if vals else 0.0

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

def wait_for_scheduler(scheduler_address, max_retries=30, delay=2):
    """Attend que le scheduler Dask soit disponible."""
    import socket
    import urllib.parse

    # Parse l'adresse pour extraire host et port
    parsed = urllib.parse.urlparse(scheduler_address)
    host = parsed.hostname or 'dask-scheduler'
    port = parsed.port or 8786

    print(f"Waiting for Dask scheduler at {host}:{port}...")

    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print(f"Scheduler is ready!")
                return True
        except Exception as e:
            pass

        time.sleep(delay)
        if i % 5 == 0:
            print(f"Still waiting... ({i}/{max_retries})")

    return False

def main():
    # 1. Configuration du client Dask
    scheduler_address = os.environ.get('DASK_SCHEDULER_ADDRESS', 'tcp://dask-scheduler:8786')

    # Attendre que le scheduler soit prêt
    if not wait_for_scheduler(scheduler_address):
        print("ERROR: Could not connect to Dask scheduler!")
        return

    # Connexion au cluster Dask
    print(f"Connecting to Dask cluster at {scheduler_address}")
    client = Client(scheduler_address)
    print(f"Connected! Dashboard link: {client.dashboard_link}")
    print(f"Number of workers: {len(client.scheduler_info()['workers'])}")

    # 2. Lecture CSV en Dask DataFrame
    csv_path = "data/transactions_autoconnect.csv"
    print(f"\nReading CSV file: {csv_path}")

    ddf = dd.read_csv(
        csv_path,
        assume_missing=True,
        dtype={
            'transaction_id': 'object',
            'date': 'object',
            'ville': 'object',
            'type': 'object',
            'modele': 'object',
            'prix': 'float64',
            'duree_location_mois': 'float64',
            'cost': 'float64'
        },
        blocksize='64MB'  # Ajuster selon la taille du fichier
    )

    print(f"Number of partitions: {ddf.npartitions}")

    # 3. Validation + reorder
    meta = pd.DataFrame({
        'transaction_id': pd.Series(dtype='object'),
        'date': pd.Series(dtype='datetime64[ns]'),
        'ville': pd.Series(dtype='object'),
        'modele': pd.Series(dtype='object'),
        'type': pd.Series(dtype='object'),
        'prix': pd.Series(dtype='float64'),
        'duree_location_mois': pd.Series(dtype='float64'),
        'cost': pd.Series(dtype='float64')
    })

    print("\nValidating data...")
    ddf_valid = ddf.map_partitions(validate_partition, meta=meta)

    # 4. Définition des calculs
    print("\nDefining calculations...")

    # Total revenue
    total_rev = ddf_valid['prix'].sum()

    # Revenue by model
    rev_by_modele = ddf_valid.groupby('modele')['prix'].sum()

    # Revenue by city
    rev_by_ville = ddf_valid.groupby('ville')['prix'].sum()

    # Average rental duration
    locs = ddf_valid[ddf_valid['type'] == 'location']
    avg_duration = locs.groupby('modele')['duree_location_mois'].mean()

    # Count by type
    count_by_type = ddf_valid.groupby('type').size()

    # Top 5 models by city
    top_models_by_city = (
        ddf_valid
        .groupby(['ville', 'modele'])['prix']
        .sum()
        .reset_index()
    )

    # 5. Exécution avec mesure de temps individuelle
    print("\n" + "="*60)
    print("STARTING DISTRIBUTED CALCULATIONS")
    print("="*60)

    # Total revenue
    t0 = time.perf_counter()
    total = total_rev.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] total_rev.compute() took {t1-t0:.4f}s")
    print(f"Total Revenue: {total:,.2f}€")

    # Revenue by model
    t0 = time.perf_counter()
    by_mod = rev_by_modele.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] rev_by_modele.compute() took {t1-t0:.4f}s")
    print("Revenue by Model:")
    for model, revenue in by_mod.sort_values(ascending=False).head(10).items():
        print(f"  {model}: {revenue:,.2f}€")

    # Revenue by city
    t0 = time.perf_counter()
    by_city = rev_by_ville.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] rev_by_ville.compute() took {t1-t0:.4f}s")
    print("Revenue by City:")
    for city, revenue in by_city.sort_values(ascending=False).items():
        print(f"  {city}: {revenue:,.2f}€")

    # Average rental duration
    t0 = time.perf_counter()
    avg_dur = avg_duration.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] avg_duration.compute() took {t1-t0:.4f}s")
    print("Average Rental Duration by Model:")
    for model, duration in avg_dur.sort_values(ascending=False).head(10).items():
        print(f"  {model}: {duration:.1f} months")

    # Count by type
    t0 = time.perf_counter()
    type_counts = count_by_type.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] count_by_type.compute() took {t1-t0:.4f}s")
    print("Transaction Count by Type:")
    for t_type, count in type_counts.items():
        print(f"  {t_type}: {count:,} transactions")

    # 6. Prévision de tendance avec mesures
    print("\n" + "="*60)
    print("TREND PREDICTION")
    print("="*60)

    # a) calcul du CA mensuel
    monthly = (
        ddf_valid
        .assign(month=ddf_valid['date'].dt.strftime("%Y-%m"))
        .groupby('month')['prix']
        .sum()
    )

    t0 = time.perf_counter()
    monthly_series = monthly.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] monthly.compute() took {t1-t0:.4f}s")
    print("Monthly Revenue:")
    for month, revenue in monthly_series.sort_index().tail(6).items():
        print(f"  {month}: {revenue:,.2f}€")

    # b) prédiction
    t0 = time.perf_counter()
    next_month, forecast = predict_trend_from_series(monthly_series.to_dict()).compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] predict_trend.compute() took {t1-t0:.4f}s")
    print(f"Forecast for {next_month}: {forecast:,.2f}€")

    # 7. Top 5 models by city
    print("\n" + "="*60)
    print("TOP 5 MODELS BY CITY")
    print("="*60)

    t0 = time.perf_counter()
    top_models_computed = top_models_by_city.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] top_models_by_city.compute() took {t1-t0:.4f}s")

    for city in top_models_computed['ville'].unique():
        city_data = top_models_computed[top_models_computed['ville'] == city]
        top_5 = city_data.nlargest(5, 'prix')
        print(f"\nTop 5 models in {city}:")
        for _, row in top_5.iterrows():
            print(f"  {row['modele']}: {row['prix']:,.2f}€")

    # 8. Sauvegarde des résultats
    print("\n" + "="*60)
    print("SAVING RESULTS")
    print("="*60)

    results_dir = "/app/results"
    os.makedirs(results_dir, exist_ok=True)

    # Sauvegarder un rapport
    report_path = os.path.join(results_dir, "analysis_report.txt")
    with open(report_path, 'w') as f:
        f.write("AUTOCONNECT SOLUTIONS - DISTRIBUTED ANALYSIS REPORT\n")
        f.write("="*60 + "\n\n")
        f.write(f"Analysis Date: {datetime.datetime.now()}\n")
        f.write(f"Total Revenue: {total:,.2f}€\n\n")

        f.write("Revenue by City:\n")
        for city, revenue in by_city.sort_values(ascending=False).items():
            f.write(f"  {city}: {revenue:,.2f}€\n")

        f.write(f"\nForecast for {next_month}: {forecast:,.2f}€\n")

    print(f"Report saved to: {report_path}")

    # Fermer le client
    print("\nClosing Dask client...")
    client.close()
    print("Done!")

if __name__ == "__main__":
    main()