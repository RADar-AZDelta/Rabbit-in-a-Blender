# Installation

RiaB requires Python 3.12 or higher.
If you want to run [DQD](https://github.com/OHDSI/DataQualityDashboard) and [Achilles](https://github.com/OHDSI/Achilles), then you need to install Java 8 or higher.
Each database engine (BigQuery, Sql Server, etc.) might also require aditional components to be installed.

- [Installation on Linux](##Installation-on-Linux)
- [Installation on Windows](##Installation-on-Windows)
- [Docker Container](##Docker-Container)


## System Requirements

The amount of CPU/RAM of the system running RiaB is dependent of the **max_parallel_tables** variable in the **riab.ini** file.
Especially when running the **--import-vocabularies** command with a high max_parallel_tables value, will result in a large CPU and RAM load of the system running RiaB (all vocabulary CSV's will be loaded into dataframes in parallel).
The other commands require less resources of the system running RiaB.

A system with 6 CPU cores and 16 GB RAM is preferred.

## Installation on Linux

### Install Python

RiaB requires Python version 3.12 or above.

Most Linux distro's come with Python preinstalled.
You can check the version of Python with:

```bash
python --version
```

If your distro has a Python version lower than 3.12, then you need to install Python 3.12. The easiest way to do this is to use [pyenv](https://github.com/pyenv/pyenv):

```bash
# install pyenv
curl https://pyenv.run | bash
``` 

Install the required packages to compile Python.
More info on the [pyenv](https://github.com/pyenv/pyenv/wiki#suggested-build-environment) site.

```bash
# Ubuntu/Debian/Mint:
apt install build-essential libssl-dev zlib1g-dev \
  libbz2-dev libreadline-dev libsqlite3-dev curl \
  libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Fedora/Redhat/CentOS:
yum install "@Development Tools" zlib-devel bzip2 bzip2-devel readline-devel sqlite \
  sqlite-devel openssl-devel xz xz-devel libffi-devel findutils tk-devel
```

Install Python 3.12.

```bash
pyenv install 3.12
# use version 3.12 of python for the current user
pyenv local 3.12 
```

### Install Java

The [Data Quality Dashboard (DQD)](https://github.com/OHDSI/DataQualityDashboard) and the [Automated Characterization of Health Information at Large-scale Longitudinal Evidence Systems (ACHILLES)](https://github.com/OHDSI/Achilles) require [Java](https://www.java.com/download) (minimal version 8)


Use the package manager of your distro, to install Java.
More info on the [Java](https://www.java.com/download) site.

```bash
# Debian, Ubuntu, etc.
apt install default-jre

# Fedora, Oracle Linux, Red Hat Enterprise Linux, etc.
yum install java-1.8.0-openjdk
```

### Install RiaB

```bash
pip install --upgrade Rabbit-in-a-Blender
```

### Optional: Install gcloud CLI

When using BigQuery as a database engine, and want to authenticate with [Application Default Credentials (ADC)](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login), you should install the [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version)

```bash
# Debian, Ubuntu, etc.
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  apt update && \
  apt install -y google-cloud-cli

# Fedora, Oracle Linux, Red Hat Enterprise Linux, etc.
tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM
[google-cloud-cli]
name=Google Cloud CLI
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el9-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOM
dnf install libxcrypt-compat.x86_64
dnf install google-cloud-cli
```

### Optional: install Sql Server bulk copy program (bcp) utility

When using Sql Server as a database engine, you need to install the Sql Server [bulk copy program (bcp)](https://learn.microsoft.com/en-us/sql/tools/bcp-utility) utility.

See [Download the latest version of the bcp utility](https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?tabs=redhat-install#install-tools-on-linux).
You also need to download and install the [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server#version-17).

```bash
# Debian, Ubuntu, etc.
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
  curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
  apt update && \
  ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools
```
> **Warning**: Under linux, the bcp command uses the current [locale](https://www.tecmint.com/set-system-locales-in-linux/) to convert floats. So make sure your current locale has a . as decimal sepertor!
```bash
localectl set-locale LC_NUMERIC=en_IN.UTF-8
```

## Installation on Windows

> **Tip**:Use a terminal that supports colors (like [Hyper](https://hyper.is/))


### Install Python

Download and install [Python](https://www.python.org/downloads/windows) 3.12 or higher.

### Install Java

Download [Java](https://www.java.com/download) and install.

### Install RiaB

```bash
pip install --upgrade Rabbit-in-a-Blender
```

#### Optional: install gcloud CLI

When using BigQuery as a database engine, and want to authenticate with [Application Default Credentials (ADC)](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login), you should install the [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk#windows)

```powershell
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
& $env:Temp\GoogleCloudSDKInstaller.exe
```

### Optional: install Sql Server bulk copy program (bcp) utility

When using Sql Server as a database engine, you need to install the Sql Server [bulk copy program (bcp)](https://learn.microsoft.com/en-us/sql/tools/bcp-utility) utility.

See [Download the latest version of the bcp utility](https://learn.microsoft.com/en-us/sql/tools/bcp-utility#windows).
You also need to download and install the [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server#version-17).

After you install the bcp utility and the ODBC Driver, you need to add the bcp utility to the PATH environment variable.

Validate that you can run the BCP command:

```powershell
bcp.exe --version
```

## Docker Container

The Docker container comes pre-installed with all the necessary components (Python, Java, BCP, Gloud CLI and RiaB).

Get the container:

```bash
docker pull ghcr.io/radar-azdelta/rabbit-in-a-blender:latest
````

Test the container:

```bash
docker run \
  --rm \
  -it \
  ghcr.io/radar-azdelta/rabbit-in-a-blender:latest --version
```

Test the database connection from the container.
We mount the riab.ini file in the container as a volume.
We set the RIAB_CONFIG environment variable to the path of the riab.ini file (within the container). 

```bash
docker run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -e RIAB_CONFIG=/riab.ini \
  ghcr.io/radar-azdelta/rabbit-in-a-blender:latest --test-db-connection
```

> **Tip**
> When using BigQuery as a database engine, and you use it with Application Default Credentials (ADC). You can mount the ADC credentials folder as volume in the container.

```bash
docker run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -e RIAB_CONFIG=/riab.ini \
  -v $HOME/.config/gcloud:/root/.config/gcloud \
  ghcr.io/radar-azdelta/rabbit-in-a-blender:latest --test-db-connection
```

Run the ETL:

```bash
docker run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -v .:/cdm_folder \
  -e RIAB_CONFIG=/riab.ini \
  ghcr.io/radar-azdelta/rabbit-in-a-blender:latest --run-etl /cdm_folder --verbose
```

> **Tip**
> You can create an alias for the container command, and put it in your .bashrc file. For example:

```bash
alias riab='docker run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -v .:/cdm_folder \
  -e RIAB_CONFIG=/riab.ini \
  ghcr.io/radar-azdelta/rabbit-in-a-blender:latest'

riab --version
riab --run-etl /cdm_folder --verbose
```