#!/usr/bin/env python3
import time
import datetime
from functools import wraps
from collections import defaultdict

import pandas as pd
import dask.dataframe as dd
from dask import delayed
from dask.distributed import Client

# ----- 1. Validation -----
class ValidationError(Exception):
    """Raised when un CSV row fails validation."""
    pass

def validate_row(row):
    """Valide et cast des champs selon votre schéma."""
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
    for row in df.to_dict(orient='records'):
        try:
            valid.append(validate_row(row))
        except ValidationError as e:
            print(f"[ERROR] {e}")
    out = pd.DataFrame(valid)
    cols = [
        'transaction_id', 'date', 'ville',
        'modele', 'type', 'prix',
        'duree_location_mois', 'cost'
    ]
    return out[cols]

@delayed
def predict_trend_from_series(monthly_rev: dict):
    months = list(monthly_rev.keys())
    vals   = list(monthly_rev.values())
    n = len(vals)
    xs = list(range(n))
    sx, sy = sum(xs), sum(vals)
    sxy    = sum(x*y for x,y in zip(xs, vals))
    sx2    = sum(x*x for x in xs)
    denom  = n*sx2 - sx*sx
    slope  = (n*sxy - sx*sy)/denom if denom else 0
    intercept = (sy - slope*sx)/n if n else 0
    last = datetime.datetime.strptime(months[-1], "%Y-%m")
    year = last.year + (last.month // 12)
    month = last.month % 12 + 1
    next_key = f"{year}-{month:02d}"
    predicted = slope*n + intercept
    return next_key, predicted

def main():
    # 1. Dask client
    client = Client()  # ou Client("tcp://scheduler:8786")
    print(client)

    # 2. Lecture CSV en Dask DataFrame
    ddf = dd.read_csv(
        "data/transactions_autoconnect.csv",
        assume_missing=True,
        dtype={
            'transaction_id':'object',
            'date':'object',
            'ville':'object',
            'type':'object',
            'modele':'object',
            'prix':'float64',
            'duree_location_mois':'float64'
        }
    )

    # 3. Validation + reorder
    meta = {
        'transaction_id':'object',
        'date':'datetime64[ns]',
        'ville':'object',
        'modele':'object',
        'type':'object',
        'prix':'float64',
        'duree_location_mois':'float64',
        'cost':'float64'
    }
    ddf_valid = ddf.map_partitions(validate_partition, meta=meta)

    # 4. Définition des calculs
    total_rev     = ddf_valid['prix'].sum()
    rev_by_modele = ddf_valid.groupby('modele')['prix'].sum()
    rev_by_ville  = ddf_valid.groupby('ville')['prix'].sum()
    locs          = ddf_valid[ddf_valid['type']=='location']
    avg_duration  = locs.groupby('modele')['duree_location_mois'].mean()

    # 5. Exécution avec mesure de temps individuelle
    # Total revenue
    t0 = time.perf_counter()
    total = total_rev.compute()
    t1 = time.perf_counter()
    print(f"[PERF] total_rev.compute() took {t1-t0:.4f}s → {total:.2f}")

    # Revenue by model
    t0 = time.perf_counter()
    by_mod = rev_by_modele.compute()
    t1 = time.perf_counter()
    print(f"[PERF] rev_by_modele.compute() took {t1-t0:.4f}s →\n{by_mod}")

    # Revenue by city
    t0 = time.perf_counter()
    by_city = rev_by_ville.compute()
    t1 = time.perf_counter()
    print(f"[PERF] rev_by_ville.compute() took {t1-t0:.4f}s →\n{by_city}")

    # Average rental duration
    t0 = time.perf_counter()
    avg_dur = avg_duration.compute()
    t1 = time.perf_counter()
    print(f"[PERF] avg_duration.compute() took {t1-t0:.4f}s →\n{avg_dur}")

    # 6. Prévision de tendance avec mesures
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
    print(f"[PERF] monthly.compute() took {t1-t0:.4f}s →\n{monthly_series}")

    # b) prédiction
    t0 = time.perf_counter()
    next_month, forecast = predict_trend_from_series(monthly_series.to_dict()).compute()
    t1 = time.perf_counter()
    print(f"[PERF] predict_trend.compute() took {t1-t0:.4f}s → {next_month} = {forecast:.2f}")

if __name__ == "__main__":
    main()
