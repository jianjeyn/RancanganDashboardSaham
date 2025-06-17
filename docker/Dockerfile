FROM apache/airflow:3.0.0

USER root

# Install system dependencies + Chromium + ChromeDriver
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    scala \
    openjdk-17-jdk \
    build-essential \
    libssl-dev \
    python3-dev \
    chromium \
    chromium-driver \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Install pip and Python dependencies
RUN curl -sSL https://bootstrap.pypa.io/pip/3.8/get-pip.py | python3 && \
    pip install --no-cache-dir --upgrade pip setuptools wheel

COPY ./docker/requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r requirements.txt
