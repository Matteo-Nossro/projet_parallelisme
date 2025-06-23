import os
import time
import datetime
import numpy as np

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
    """Prédiction de tendance sur les 3 derniers mois."""
    if not monthly_rev or len(monthly_rev) < 3:
        return "N/A", 0.0

    # Prendre les 3 derniers mois
    months = sorted(list(monthly_rev.keys()))[-3:]
    vals = [monthly_rev[m] for m in months]
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

def wait_for_scheduler(scheduler_address, max_retries=30, delay=2):
    """Attend que le scheduler Dask soit disponible."""
    import socket
    import urllib.parse

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

    if not wait_for_scheduler(scheduler_address):
        print("ERROR: Could not connect to Dask scheduler!")
        return

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
        blocksize='64MB'
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

    # 1. Chiffre d'affaires mensuel par ville (vente + location)
    monthly_revenue_by_city = (
        ddf_valid
        .assign(month=ddf_valid['date'].dt.strftime("%Y-%m"))
        .groupby(['month', 'ville'])['prix']
        .sum()
        .reset_index()
    )

    # 2. Répartition vente/location par ville
    type_distribution_by_city = (
        ddf_valid
        .groupby(['ville', 'type'])
        .agg({'prix': 'sum', 'transaction_id': 'count'})
        .reset_index()
    )

    # 3. Performance comparative inter-villes
    city_performance = ddf_valid.groupby('ville').agg({
        'prix': ['sum', 'mean', 'count'],
        'transaction_id': 'count'
    })

    # ===== CALCULS SUPPLÉMENTAIRES =====

    # 4. Top 5 des modèles par ville (ventes ET locations)
    top_models_by_city = (
        ddf_valid
        .groupby(['ville', 'modele', 'type'])
        .agg({'prix': 'sum', 'transaction_id': 'count'})
        .reset_index()
    )

    # 5. Analyse de saisonnalité
    monthly_revenue = (
        ddf_valid
        .assign(month=ddf_valid['date'].dt.strftime("%Y-%m"))
        .groupby('month')['prix']
        .agg(['sum', 'count', 'mean'])
        .reset_index()
    )

    # 6. Temps moyen de rotation (durée moyenne de location)
    avg_rental_duration = (
        ddf_valid[ddf_valid['type'] == 'location']
        .groupby(['ville', 'modele'])['duree_location_mois']
        .agg(['mean', 'std', 'count'])
        .reset_index()
    )

    # 7. Marge bénéficiaire (si cost est disponible)
    profit_margins = ddf_valid.copy()
    profit_margins['margin'] = profit_margins['prix'] - profit_margins['cost'].fillna(0)
    profit_margins['margin_rate'] = (profit_margins['margin'] / profit_margins['prix'] * 100).fillna(0)

    margin_by_type_city = profit_margins.groupby(['ville', 'type']).agg({
        'margin': ['sum', 'mean'],
        'margin_rate': 'mean'
    })

    # ===== CALCULS ADDITIONNELS =====

    # Total revenue
    total_rev = ddf_valid['prix'].sum()

    # Revenue by model (global)
    rev_by_modele = ddf_valid.groupby('modele')['prix'].sum()

    # Revenue by city (global)
    rev_by_ville = ddf_valid.groupby('ville')['prix'].sum()

    # Count by type
    count_by_type = ddf_valid.groupby('type').size()

    # 5. Exécution avec mesure de temps
    print("\n" + "="*60)
    print("STARTING ALL DISTRIBUTED CALCULATIONS")
    print("="*60)

    results = {}

    # CALCULS REQUIS
    print("\n--- CALCULS REQUIS ---")

    # 1. CA mensuel par ville
    t0 = time.perf_counter()
    monthly_by_city = monthly_revenue_by_city.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] Monthly revenue by city: {t1-t0:.4f}s")
    results['monthly_revenue_by_city'] = monthly_by_city

    # 2. Répartition vente/location par ville
    t0 = time.perf_counter()
    type_dist = type_distribution_by_city.compute()
    t1 = time.perf_counter()
    print(f"[PERF] Type distribution by city: {t1-t0:.4f}s")
    results['type_distribution'] = type_dist

    # 3. Performance comparative
    t0 = time.perf_counter()
    city_perf = city_performance.compute()
    t1 = time.perf_counter()
    print(f"[PERF] City performance comparison: {t1-t0:.4f}s")
    results['city_performance'] = city_perf

    # CALCULS SUPPLÉMENTAIRES
    print("\n--- CALCULS SUPPLÉMENTAIRES ---")

    # 4. Top 5 modèles par ville
    t0 = time.perf_counter()
    top_models = top_models_by_city.compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] Top models by city: {t1-t0:.4f}s")
    results['top_models'] = top_models

    # 5. Analyse mensuelle (saisonnalité)
    t0 = time.perf_counter()
    monthly_analysis = monthly_revenue.compute()
    t1 = time.perf_counter()
    print(f"[PERF] Monthly analysis: {t1-t0:.4f}s")
    results['monthly_analysis'] = monthly_analysis

    # 6. Durée moyenne de location
    t0 = time.perf_counter()
    rental_duration = avg_rental_duration.compute()
    t1 = time.perf_counter()
    print(f"[PERF] Rental duration analysis: {t1-t0:.4f}s")
    results['rental_duration'] = rental_duration

    # 7. Marges bénéficiaires
    t0 = time.perf_counter()
    margins = margin_by_type_city.compute()
    t1 = time.perf_counter()
    print(f"[PERF] Profit margins: {t1-t0:.4f}s")
    results['profit_margins'] = margins

    # Calculs globaux
    t0 = time.perf_counter()
    total = total_rev.compute()
    by_city = rev_by_ville.compute()
    by_model = rev_by_modele.compute()
    by_type = count_by_type.compute()
    t1 = time.perf_counter()
    print(f"[PERF] Global calculations: {t1-t0:.4f}s")

    # 6. Affichage des résultats
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)

    print(f"\nTOTAL REVENUE: {total:,.2f}€")

    print("\n1. CHIFFRE D'AFFAIRES MENSUEL PAR VILLE:")
    pivot_monthly = monthly_by_city.pivot(index='month', columns='ville', values='prix').fillna(0)
    for month in sorted(pivot_monthly.index)[-6:]:  # 6 derniers mois
        print(f"\n{month}:")
        for city in pivot_monthly.columns:
            print(f"  {city}: {pivot_monthly.loc[month, city]:,.2f}€")

    print("\n2. RÉPARTITION VENTE/LOCATION PAR VILLE:")
    for ville in type_dist['ville'].unique():
        ville_data = type_dist[type_dist['ville'] == ville]
        print(f"\n{ville}:")
        total_ville = ville_data['prix'].sum()
        for _, row in ville_data.iterrows():
            pct = (row['prix'] / total_ville * 100) if total_ville > 0 else 0
            print(f"  {row['type']}: {row['prix']:,.2f}€ ({pct:.1f}%) - {row['transaction_id']} transactions")

    print("\n3. PERFORMANCE COMPARATIVE INTER-VILLES:")
    city_perf.columns = ['_'.join(col).strip() if col[1] else col[0] for col in city_perf.columns.values]
    for city, row in city_perf.iterrows():
        print(f"\n{city}:")
        print(f"  Total: {row['prix_sum']:,.2f}€")
        print(f"  Moyenne: {row['prix_mean']:,.2f}€")
        print(f"  Nombre: {int(row['prix_count'])} transactions")

    print("\n4. TOP 5 MODÈLES PAR VILLE:")
    for ville in top_models['ville'].unique():
        ville_models = top_models[top_models['ville'] == ville]
        top_5 = ville_models.groupby('modele')['prix'].sum().nlargest(5)
        print(f"\n{ville}:")
        for i, (model, revenue) in enumerate(top_5.items(), 1):
            print(f"  {i}. {model}: {revenue:,.2f}€")

    print("\n5. ANALYSE DE SAISONNALITÉ:")
    print("Variation mensuelle du CA:")
    monthly_sorted = monthly_analysis.sort_values('month')
    for _, row in monthly_sorted.tail(12).iterrows():  # 12 derniers mois
        print(f"  {row['month']}: {row['sum']:,.2f}€ ({row['count']} transactions, moy: {row['mean']:,.2f}€)")

    # Calcul de la variation
    if len(monthly_sorted) >= 2:
        revenues = monthly_sorted['sum'].values
        variations = [(revenues[i] - revenues[i-1]) / revenues[i-1] * 100
                      for i in range(1, len(revenues))]
        avg_variation = np.mean(variations) if variations else 0
        print(f"\nVariation moyenne mensuelle: {avg_variation:+.1f}%")

    print("\n6. TEMPS MOYEN DE ROTATION (DURÉE LOCATION):")
    avg_duration_global = rental_duration['mean'].mean()
    print(f"Durée moyenne globale: {avg_duration_global:.1f} mois")
    print("\nPar ville:")
    for ville in rental_duration['ville'].unique():
        ville_dur = rental_duration[rental_duration['ville'] == ville]
        avg_ville = ville_dur['mean'].mean()
        print(f"  {ville}: {avg_ville:.1f} mois")

    print("\n7. MARGES BÉNÉFICIAIRES:")
    if not margins.empty:
        margins.columns = ['_'.join(col).strip() if col[1] else col[0] for col in margins.columns.values]
        for (ville, type_), row in margins.iterrows():
            print(f"\n{ville} - {type_}:")
            print(f"  Marge totale: {row.get('margin_sum', 0):,.2f}€")
            print(f"  Marge moyenne: {row.get('margin_mean', 0):,.2f}€")
            print(f"  Taux de marge: {row.get('margin_rate_mean', 0):.1f}%")

    # 7. Prédiction de tendance
    print("\n" + "="*60)
    print("TREND PREDICTION")
    print("="*60)

    monthly_total = monthly_analysis.set_index('month')['sum'].to_dict()
    t0 = time.perf_counter()
    next_month, forecast = predict_trend_from_series(monthly_total).compute()
    t1 = time.perf_counter()
    print(f"\n[PERF] Trend prediction: {t1-t0:.4f}s")
    print(f"Prévision pour {next_month}: {forecast:,.2f}€")

    # 8. Sauvegarde du rapport complet
    print("\n" + "="*60)
    print("SAVING COMPLETE REPORT")
    print("="*60)

    results_dir = "/app/results"
    os.makedirs(results_dir, exist_ok=True)

    report_path = os.path.join(results_dir, "complete_analysis_report.txt")
    with open(report_path, 'w') as f:
        f.write("AUTOCONNECT SOLUTIONS - RAPPORT D'ANALYSE DISTRIBUÉ COMPLET\n")
        f.write("="*80 + "\n")
        f.write(f"Date d'analyse: {datetime.datetime.now()}\n")
        f.write(f"Architecture: Dask Distributed Computing\n")
        f.write("="*80 + "\n\n")

        f.write("RÉSUMÉ EXÉCUTIF\n")
        f.write("-"*80 + "\n")
        f.write(f"Chiffre d'affaires total: {total:,.2f}€\n")
        f.write(f"Nombre de villes: {len(by_city)}\n")
        f.write(f"Nombre de modèles: {len(by_model)}\n")
        f.write(f"Prévision mois prochain ({next_month}): {forecast:,.2f}€\n\n")

        f.write("1. CHIFFRE D'AFFAIRES MENSUEL PAR VILLE\n")
        f.write("-"*80 + "\n")
        for month in sorted(pivot_monthly.index)[-6:]:
            f.write(f"\n{month}:\n")
            for city in pivot_monthly.columns:
                f.write(f"  {city}: {pivot_monthly.loc[month, city]:,.2f}€\n")

        f.write("\n2. RÉPARTITION VENTE/LOCATION PAR VILLE\n")
        f.write("-"*80 + "\n")
        for ville in type_dist['ville'].unique():
            ville_data = type_dist[type_dist['ville'] == ville]
            f.write(f"\n{ville}:\n")
            total_ville = ville_data['prix'].sum()
            for _, row in ville_data.iterrows():
                pct = (row['prix'] / total_ville * 100) if total_ville > 0 else 0
                f.write(f"  {row['type']}: {row['prix']:,.2f}€ ({pct:.1f}%)\n")

        f.write("\n3. PERFORMANCE COMPARATIVE INTER-VILLES\n")
        f.write("-"*80 + "\n")
        for city, revenue in by_city.sort_values(ascending=False).items():
            pct = (revenue / total * 100) if total > 0 else 0
            f.write(f"  {city}: {revenue:,.2f}€ ({pct:.1f}% du total)\n")

        f.write("\n4. TOP 5 MODÈLES PAR VILLE\n")
        f.write("-"*80 + "\n")
        for ville in top_models['ville'].unique():
            ville_models = top_models[top_models['ville'] == ville]
            top_5 = ville_models.groupby('modele')['prix'].sum().nlargest(5)
            f.write(f"\n{ville}:\n")
            for i, (model, revenue) in enumerate(top_5.items(), 1):
                f.write(f"  {i}. {model}: {revenue:,.2f}€\n")

        f.write("\n5. ANALYSE DE SAISONNALITÉ\n")
        f.write("-"*80 + "\n")
        f.write("Évolution mensuelle (12 derniers mois):\n")
        for _, row in monthly_sorted.tail(12).iterrows():
            f.write(f"  {row['month']}: {row['sum']:,.2f}€\n")

        f.write("\n6. DURÉE MOYENNE DE LOCATION\n")
        f.write("-"*80 + "\n")
        f.write(f"Moyenne globale: {avg_duration_global:.1f} mois\n")
        for ville in rental_duration['ville'].unique():
            ville_dur = rental_duration[rental_duration['ville'] == ville]
            avg_ville = ville_dur['mean'].mean()
            f.write(f"  {ville}: {avg_ville:.1f} mois\n")

        f.write("\n7. MARGES BÉNÉFICIAIRES\n")
        f.write("-"*80 + "\n")
        if not margins.empty:
            for (ville, type_), row in margins.iterrows():
                f.write(f"\n{ville} - {type_}:\n")
                f.write(f"  Taux de marge moyen: {row.get('margin_rate_mean', 0):.1f}%\n")

        f.write("\n" + "="*80 + "\n")
        f.write("Rapport généré avec succès\n")

    print(f"Rapport complet sauvegardé: {report_path}")

    # Sauvegarder aussi les données en CSV pour analyse ultérieure
    csv_dir = os.path.join(results_dir, "csv_exports")
    os.makedirs(csv_dir, exist_ok=True)

    # Export des principaux résultats
    monthly_by_city.to_csv(os.path.join(csv_dir, "monthly_revenue_by_city.csv"), index=False)
    type_dist.to_csv(os.path.join(csv_dir, "type_distribution_by_city.csv"), index=False)
    top_models.to_csv(os.path.join(csv_dir, "top_models_by_city.csv"), index=False)
    monthly_analysis.to_csv(os.path.join(csv_dir, "monthly_analysis.csv"), index=False)
    rental_duration.to_csv(os.path.join(csv_dir, "rental_duration_analysis.csv"), index=False)

    print(f"Fichiers CSV exportés dans: {csv_dir}")

    print("\nFermeture du client Dask...")
    client.close()
    print("Analyse terminée avec succès!")

if __name__ == "__main__":
    main()