FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY source/debezium_configurator/ /app/
COPY source/utilities/ /app/utilities/

# Copy data
COPY config/debezium/connector_config.json /app/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the configuration script
CMD ["python", "/app/debezium_configurator.py"]