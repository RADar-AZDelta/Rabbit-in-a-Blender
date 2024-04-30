FROM docker.io/python:3.12-slim-bookworm

ARG MSODBC_VERSION=17

LABEL org.opencontainers.image.authors="radar@azdelta.be"

# update APT and install required tools
RUN apt update && \
  apt install -y --no-install-recommends apt-transport-https ca-certificates gnupg curl debian-keyring

# Install JAVA
RUN apt install -y --no-install-recommends default-jre 

# Install Google Cloud CLI
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  apt update && \
  apt install -y google-cloud-cli

# Install SQL Server tools
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
  curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
  apt update && \
  ACCEPT_EULA=Y apt-get install -y msodbcsql${MSODBC_VERSION} mssql-tools

# Cleanup APT cache
RUN apt clean && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools/bin"

# install RiaB
RUN python -m pip install --no-cache-dir Rabbit-in-a-Blender

ENV RIAB_CONFIG="/cdm_folder/riab.ini"
WORKDIR /cdm_folder

ENTRYPOINT ["/usr/local/bin/riab"]