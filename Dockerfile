# Dockerfile

# 1. Base image with Python
FROM python:3.10-slim

# 2. Workdir inside container
WORKDIR /app

# 3. Copy only what we need
COPY main.py .

# Si tu as un requirements.txt, décommente ces lignes :
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# 4. Default value, surchargée par docker-compose via NUM_WORKERS
ENV NUM_WORKERS=1

# 5. Entrypoint + commande par défaut
ENTRYPOINT ["python", "main.py"]
CMD ["data/transactions_autoconnect.csv"]
