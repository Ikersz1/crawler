# Imagen oficial de Playwright con Python y navegadores ya instalados
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

WORKDIR /app

# Instala dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código del backend
COPY . .

# (opcional) carpeta de salida local
RUN mkdir -p /app/output_crawler

ENV PYTHONUNBUFFERED=1
ENV PORT=8000

EXPOSE 8000
CMD ["bash", "-lc", "uvicorn crawler_api:app --host 0.0.0.0 --port ${PORT}"]
