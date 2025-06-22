.PHONY: help build up down logs clean dashboard scale

help:
	@echo "Commandes disponibles:"
	@echo "  make build     - Construire les images Docker"
	@echo "  make up        - Démarrer le cluster"
	@echo "  make down      - Arrêter le cluster"
	@echo "  make logs      - Voir les logs"
	@echo "  make clean     - Nettoyer tout"
	@echo "  make dashboard - Ouvrir le dashboard Dask"
	@echo "  make scale     - Ajouter des workers"

build:
	docker-compose build

up:
	docker-compose up -d
	@echo "Cluster démarré!"
	@echo "Dashboard Dask: http://localhost:8787"
	@echo "Attendre 10-15 secondes avant de lancer le client..."

down:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	docker system prune -f

dashboard:
	@echo "Ouverture du dashboard Dask..."
	@python -m webbrowser http://localhost:8787

# Lancer uniquement l'analyse (après que le cluster soit démarré)
run-analysis:
	docker-compose run --rm client

# Voir les logs du client uniquement
client-logs:
	docker-compose logs -f client

# Ajouter plus de workers (scale up)
scale:
	docker-compose up -d --scale dask-worker-1=2 --scale dask-worker-2=2 --scale dask-worker-3=2

# Vérifier l'état du cluster
status:
	docker-compose ps

# Créer le dossier results s'il n'existe pas
prepare:
	mkdir -p results data