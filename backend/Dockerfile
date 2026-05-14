FROM python:3.12-slim

WORKDIR /app

# System-Abhängigkeiten für pyproj
RUN apt-get update && apt-get install -y --no-install-recommends \
    proj-bin libproj-dev \
    libhdf5-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend

ENV PYTHONPATH=/app/backend
ENV VELO_DB_PATH=/data/velo.db
VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/app/backend"]
