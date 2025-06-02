FROM bitnami/spark:3.5

USER root
WORKDIR /app

# Install Python 3.11 and build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.11 as default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/bin/python3.11

# Upgrade pip
RUN python3 -m pip install --upgrade pip

# Copy files
COPY source/spark_processor/ /app/
COPY source/utilities/ /app/utilities/
COPY .env /app/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r utilities/requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Run the spark processor
CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "--conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore", \
     "/app/spark_processor.py"]
