# Use a specific Airflow image as the base
FROM apache/airflow:2.7.0

# Switch to the root user to install system-level packages
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    # Add any other system libraries you might need
    && rm -rf /var/lib/apt/lists/*

# Switch back to the 'airflow' user
USER airflow

# Copy your requirements file and install the dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt