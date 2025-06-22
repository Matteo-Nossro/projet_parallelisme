# Système de Calcul Distribué - AutoConnect

## Architecture

Ce projet implémente un système de calcul distribué utilisant **Dask** pour analyser les performances commerciales d'AutoConnect Solutions.

### Composants:
- **1 Scheduler Dask** : Coordonne les tâches
- **3 Workers Dask** : Exécutent les calculs en parallèle
- **1 Client** : Lance l'analyse
- **Dashboard Web** : Surveillance en temps réel (http://localhost:8787)

## Installation et Démarrage

### 1. Préparation de l'environnement

```bash
# Créer les dossiers nécessaires
mkdir -p data results

# Placer le fichier CSV dans le dossier data/
```

### 2. Construction des images

```bash
make build
# ou
docker-compose build
```

### 3. Démarrage du cluster

```bash
make up
# ou
docker-compose up -d
```

Attendre un peu (10-15 secondes) que le cluster soit complètement démarré.

### 4. Vérification du cluster

```bash
# Vérifier l'état des conteneurs
make status
# ou
docker-compose ps

# Voir les logs en temps réel
make logs
# ou
docker-compose logs -f
```

### 5. Lancement de l'analyse

Le client se lance automatiquement avec le cluster. Pour relancer l'analyse :

```bash
make run-analysis
# ou
docker-compose run --rm client
```

### 6. Consultation des résultats

- **Dashboard Dask** : http://localhost:8787
- **Rapport d'analyse** : `results/analysis_report.txt`
- **Logs du client** : `make client-logs`

## Commandes Utiles

```bash
# Arrêter le cluster
make down

# Nettoyer tout (conteneurs, volumes, images)
make clean

# Ajouter plus de workers (scale up)
make scale

# Voir uniquement les logs du client
make client-logs
```

## Métriques de Performance

Le système affiche les temps d'exécution pour chaque calcul :
- Chiffre d'affaires total
- CA par modèle et par ville
- Durée moyenne de location
- Prévisions de tendance
- Top 5 des modèles par ville

## Surveillance

Le dashboard Dask (http://localhost:8787) permet de surveiller :
- L'utilisation CPU/RAM par worker
- La distribution des tâches
- Les temps d'exécution
- Les graphiques de performance en temps réel