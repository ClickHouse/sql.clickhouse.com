# Use the latest Python 3 base image
FROM python:3.12

# Install curl and gsutil dependencies
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install gsutil (part of Google Cloud SDK)
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update -y \
    && apt-get install -y google-cloud-sdk \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy updater.py
COPY updater.py .

# Download source.tar.gz into the container
RUN curl -O https://storage.googleapis.com/hyperdx/source.tar.gz

# Copy run.sh and make it executable
COPY run.sh .
RUN chmod +x run.sh

# Set the entrypoint
ENTRYPOINT ["./run.sh"]
