FROM clickhouse

WORKDIR /home
RUN apt-get update && apt-get install -y \ 
    jq \
    curl \
    gpg \
    unzip
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg |  gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y
COPY ingest.sh /home/ingest.sh
RUN chmod +x /home/ingest.sh

ENTRYPOINT ["./ingest.sh"]


