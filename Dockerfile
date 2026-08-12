FROM python:3.11-slim

# Spark needs a JVM even in local mode.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

# Data and GCP service-account key are mounted at runtime, not baked into the image:
#   docker run -v $(pwd)/data:/app/data -v $(pwd)/keys:/app/keys \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/app/keys/sa-key.json \
#     olist-pipeline --data-dir data/raw --load-to-bq --bq-project my-project
ENTRYPOINT ["python3", "src/pipeline.py"]
CMD ["--data-dir", "data/raw", "--out-dir", "data/output"]
