FROM python:3.11-slim

WORKDIR /app

# Install system deps (needed for psycopg2)
RUN apt-get update && apt-get install -y gcc libpq-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scripts/run_elt_pipeline.py"]