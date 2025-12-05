FROM apache/spark:3.5.0

# Install Python dependencies and network tools required by streaming app
USER root

# Install system dependencies (netcat for health checks, gosu for user switching)
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd gosu && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages required by streaming app
# Note: pandas may not be available in the Spark Python environment
RUN pip install --no-cache-dir requests pandas pyarrow

# Switch back to spark user
USER spark

