FROM docker.io/python:3.12-slim-bookworm

ARG MSODBC_VERSION=17

LABEL org.opencontainers.image.authors="radar@azdelta.be"

# Install MSSQL
RUN apt update && \
  apt install -y --no-install-recommends curl ca-certificates debian-keyring gnupg && \
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
  curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
  apt update && \
  ACCEPT_EULA=Y apt-get install -y msodbcsql${MSODBC_VERSION} mssql-tools && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools/bin"

# install riab
RUN python -m pip install --no-cache-dir Rabbit-in-a-Blender

ENTRYPOINT ["/usr/local/bin/riab"]