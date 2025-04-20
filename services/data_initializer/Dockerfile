FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY services/data_initializer/ /app/

# Copy data
COPY data/input/ /app/data/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the business simulator
CMD ["python", "/app/data_initializer.py"]
