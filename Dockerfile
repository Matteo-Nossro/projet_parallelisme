# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copie et installation des dépendances Python
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copie des fichiers de l'application
COPY main.py ./
COPY data/ ./data/

# Variable d'environnement pour Dask
ENV DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=60s
ENV DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=60s

# Par défaut, lance le script principal
CMD ["python", "main.py"]