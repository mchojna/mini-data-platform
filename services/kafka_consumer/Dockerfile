FROM python:3.11-slim

WORKDIR /app

# Copy files
COPY source/kafka_consumer/ /app/
COPY source/utilities/ /app/utilities/
COPY .env /app/

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r utilities/requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the Kafka consumer
CMD ["python", "/app/kafka_consumer.py"]
