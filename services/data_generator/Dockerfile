FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY source/data_generator/ /app/
COPY source/utilities/ /app/utilities/
COPY .env /app/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r utilities/requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the business simulator
CMD ["python", "/app/data_generator.py"]
    