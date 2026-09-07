FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-postgres==1.11.0

WORKDIR /opt/fastorder-dbt

CMD ["sleep", "infinity"]