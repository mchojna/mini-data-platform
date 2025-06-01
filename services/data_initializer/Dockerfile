FROM python:3.13.3-alpine

WORKDIR /app

# Copy files
COPY source/data_initializer/ /app/
COPY source/utilities/ /app/utilities/

# Copy data
COPY data/input/ /app/data/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r utilities/requirements.txt

# Set Python path to include scripts directory
ENV PYTHONPATH=/app

# Run the data initalizer
CMD ["python", "/app/data_initializer.py"]