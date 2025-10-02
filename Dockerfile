# Imagen oficial de Playwright para Python (incluye Chromium/Firefox/WebKit y deps)
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /app

# Instala Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# (Opcional) Si tu código usa fuentes del SO o tzdata:
# RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*

# Copia tu backend
COPY . .

# Variables para Uvicorn
ENV PORT=8000
ENV PYTHONUNBUFFERED=1

# Arranca FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
