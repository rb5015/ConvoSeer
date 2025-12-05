FROM apache/spark:3.5.0

# Install Python dependencies required by Spark executors for Pandas UDFs
USER root

# Install Python packages required by streaming app
# Note: pandas must be available on executors for Pandas UDFs to work
RUN pip install --no-cache-dir requests pandas pyarrow

# Switch back to spark user
USER spark

