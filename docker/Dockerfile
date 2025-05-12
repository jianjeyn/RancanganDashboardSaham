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