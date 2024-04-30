# Installation

RiaB requires Python 3.12 or higher.
If you want to run DQD and Achilles, then you need to install Java 8 or higher.

- [Installation on Linux](#Installation-on-Linux)
- [Installation on Windows](#Installation-on-Windows)
- [Docker Container](#Docker-Container)


## System Requirements

The amount of CPU/RAM of the system running RiaB is dependent of the **max_parallel_tables** variable in the **riab.ini** file.
Running the --import-vocabularies with a high max_parallel_tables value, will result in a large CPU and RAM load of the system running RiaB.
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

## Installation on Windows

### Install Python

Download [Python] 3.12 or higher (https://www.python.org/downloads/windows) and install.

### Install Java

Download [Java](https://www.java.com/download) and install.

### Install RiaB

```bash
pip install --upgrade Rabbit-in-a-Blender
```

## Docker Container

The Docker container comes preinstalled with Python, Java, BCP and RiaB.
