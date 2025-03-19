FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3000

# Gunicorn ile Flask uygulamasını başlat
CMD ["gunicorn", "-b", "0.0.0.0:3000", "-w", "4", "main:app"]
