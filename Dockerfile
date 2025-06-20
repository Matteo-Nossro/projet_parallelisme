# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copie ton script principal
COPY main.py ./
COPY data/ ./data/

CMD ["python", "main.py"]
