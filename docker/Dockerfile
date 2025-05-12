FROM apache/airflow:3.0.0

USER root

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    scala \
    openjdk-17-jdk \
    build-essential \
    libssl-dev \
    python3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Install Python dependencies
RUN curl -sSL https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3 && \
    pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy the requirements file
COPY ./docker/requirements.txt /opt/airflow/

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create the /opt/jars directory and download the MongoDB Spark Connector
RUN mkdir -p /opt/airflow/jars && \
    curl -o /opt/airflow/jars/mongo-spark-connector.jar \
        https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar && \
    curl -o /opt/airflow/jars/mongodb-driver-sync.jar \
        https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.0.5/mongodb-driver-sync-4.0.5.jar && \
    curl -o /opt/airflow/jars/mongodb-driver-core.jar \
        https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.0.5/mongodb-driver-core-4.0.5.jar && \
    curl -o /opt/airflow/jars/bson.jar \
        https://repo1.maven.org/maven2/org/mongodb/bson/4.0.5/bson-4.0.5.jar