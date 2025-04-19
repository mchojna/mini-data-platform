FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY services/data_generator/src/ ./src/

# Install required packages
RUN pip install --no-cache-dir -r src/requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the business simulator
CMD ["python", "/app/src/data_generator.py"]
    