FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY services/debezium_configurator/ /app/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the configuration script
CMD ["python", "debezium_configurator.py"]