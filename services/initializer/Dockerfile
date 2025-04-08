FROM python:3.12-slim

WORKDIR /app

# Copy files
COPY services/initializer/ .

# Install required packages
RUN pip install --no-cache-dir -r src/requirements.txt

# Copy data
COPY data/input/ /app/data/

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the business simulator
CMD ["python", "/app/src/initializer.py"]
