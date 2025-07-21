# Dockerfile
FROM jeft-docker.artifactrepo.jnj.com/jph:dev.v1.2

# Switch to root to install packages
USER root

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=/app:$PYTHONPATH

# Install system dependencies and upgrade pip
RUN apt-get update && \
    apt-get install -y curl wget && \
    python3 -m pip install --upgrade pip && \
    rm -rf /var/lib/apt/lists/*

# Create application directory
RUN mkdir -p /app /app/logs && \
    chown -R spark:spark /app

# Download Redshift JDBC driver
RUN mkdir -p /opt/spark/jars && \
    wget -q https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.29/redshift-jdbc42-2.1.0.29.jar \
    -O /opt/spark/jars/redshift-jdbc42-2.1.0.29.jar

RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar \
    -O /opt/spark/jars/aws-java-sdk-bundle-1.12.517.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -O /opt/spark/jars/hadoop-aws-3.3.4.jar

# Set working directory
WORKDIR /app

# Copy application code
#COPY . /app
COPY spark-etl-agent/ /app/


# Copy requirements and install Python dependencies
COPY _scm_config/sparkImageRequirements/requirements.txt /app/
#COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Fix permissions
RUN chown -R spark:spark /app

# Switch back to spark user
USER spark

# Set default environment variables
ENV JOB_ID=1
ENV LOAD_DATE=2025-05-19
ENV LOG_LEVEL=INFO
ENV LIMIT=10

ENV AWS_SECRET_NAME=jph-eks-dev-secret
ENV AWS_DEFAULT_REGION=us-east-1

# Expose Spark UI port
EXPOSE 4040

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python3 -c "import pyspark; print('OK')" || exit 1

# Default command - run the application in local mode
CMD ["python3", "app.py", "--local", "--continuous","--job-type", "control_m_poc_etl", "--job-id", "3", "--load-date", "2025-05-19"]