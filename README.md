![Rabbit in a Blender logo](resources/img/rabbitinablenderlogo.png)
===========

**Rabbit in a Blender** is an ETL pipeline to transform your EMP data to OMOP.

Introduction
============

Extract-Transform-Load (ETL) processes are very complex and are mainly crafted by highly skilled data engineers. The process of transforming the electronic medical record (EMR) data into the observational medical outcomes partnership (OMOP) common data model (CDM) is no exception. The mapping process of the source values to standard concepts is mostly done by subject matter experts, who lack the knowledge of programming the ETL process. Wouldn’t it be nice if we could drastically simplify the ETL process, so that you don’t need seasoned data engineers to start the OMOP CDM journey. Imagine that you just save your queries, Usagi comma separated value (CSV) text files and custom concept CSV’s on disk, and run a command line interface (CLI) tool that does all the ETL magic automatically. 


Concept
=======
The main strength of the CDM is its simplified scheme. This scheme is a relational data model, where each table has a primary key and can have foreign keys to other tables. Because of the relational data model, we can extract the dependencies of the tables from the scheme. For example, the provider table is dependent on the care_site table, which is in its turn dependent on the location table. If we flatten that dependency graph, we have a sequence of ETL steps that we need to follow to have consistent data in our OMOP CDM. These ETL steps can be automated, so a hospital can focus its resources on the queries and the mapping of the concepts. The automated ETL consists of multiple tasks. It needs to execute queries, add custom concepts, apply the Usagi source to concept mapping, and do a lot of housekeeping. An example of that housekeeping is the autonumbering of the OMOP CDM primary keys, for which the ETL process needs to maintain a swap table that holds the key of the source table and the generated sequential number of the CDM table’s primary key. Another example of the housekeeping is the upload and processing of the Usagi CSV’s and also the upload and parsing of the custom concept CSV’s. In an ETL process data is divided in zones. The raw zone holds the source data (for example the data from the EMR), the work zone holds all the house keeping tables of the ETL process and the gold zone holds our final OMOP CDM.
After designing the architecture, the implementation needs to be developed. We have two options to choose from: configuration and convention as design paradigm. We choose convention over configuration6, because it decreases the number of decisions the user has to make and eliminates the complexity. As convention a specific folder structure is adopted (see [our mappings as example](https://github.com/RADar-AZDelta/AZDelta-OMOP-CDM)). A folder is created for each OMOP CDM table, where the SQL queries are stored to fill up the specific CDM table. In the table folders we also have for each concept column a sub folder. Those concept column sub folders hold our Usagi CSV’s (files ending with _usagi.csv). We also have a custom folder in the concept column sub folder, that holds the custom concept CSV’s (files ending with _concept.csv). With this convention in place, our ETL CLI tool has everything it needs to do its magic.
One final requirement we want to build in the ETL CLI tool, is that each ETL step is an atomic operation, it either fails or succeeds, so that there is no possibility to corrupt the final OMOP CDM data.

Remarks
=======
You will need to run the cleanup command, when concept mappings change in your existing Usagi CSV's. The cleanup is not necessary when you add new queries or add additional Usagi mappings.

The fact_relationship table has no primary key, which makes it difficult for the ETL process to trace the source of the data and very hard to figure out if it is new or updated data. So running the cleanup command frequently for this table is advised.
The filenames of queries must also have the folowing convention: **_{fact_id_1_foreign_key_table}_{fact_id_1_foreign_key_table}.sql** (ex: patient_relationship_person_person.sql), so that the ETL process knows to what tables the fact_id_1 and fact_id_2 are referring to.

For the moment we only implemented a BigQuery backend for the ETL process, because this is what our hospital uses. Other database technologies as ETL backend can be implemented.

CLI Usage
========


* **Options**:
    |  command | help  
    |---|---
    | -h, --help | Show help message and exit
    | -v, --verbose | Verbose logging (logs are also writen to a log file in the systems tmp folder)
    | --create-db | Create the OMOP CDM tables
    | -i, --import-vocabularies [VOCABULARIES_ZIP_FILE] | Extracts the vocabulary zip file (downloaded from the Athena website) and imports it into the OMOP CDM database.
    | -c, --cleanup [TABLE] | Cleanup all the OMOP tables, or just one. Be aware that the cleanup of a single table can screw up foreign keys! For instance cleaning up only the 'Person' table, will result in clicical results being mapped to the wrong persons!!!!
    | -t [TABLE], --table [TABLE] | Do only ETL on this specific OMOP CDM table
    | -s, --skip-usagi-and-custom-concept-upload | Skips the parsing and uploading of the Usagi and custom concept CSV's

* **Required named arguments:**
    |  command | help  
    |---|---
    | -d [DB-ENGINE], --db-engine [DB-ENGINE] | The database engine technology the ETL is running on. Each database engine has its own legacy SQL dialect, so the generated ETL queries can be different for each database engine. For the moment only BigQuery is supported, yet 'Rabbit in a Blender' has an open design, so in the future other database engines can be added easily.
    | PATH | Path to the folder structure that holds the queries, Usagi CSV's and the custom concept CSV's

* **Bigquery specific options:**
    |  command | help  
    |---|---
    | --google-credentials-file [GOOGLE_CREDENTIALS_FILE] | Loads Google credentials from a file.
    | --google-project-id [GOOGLE_PROJECT_ID] | The Google GCP project id
    | --google-location [GOOGLE_LOCATION] | The google locations to store the data (see https://cloud.google.com/about/locations)
    | --bigquery-dataset-id-raw [BIGQUERY_DATASET_ID_RAW] | BigQuery dataset that holds the raw EMR data
    | --bigquery-dataset-id-work [BIGQUERY_DATASET_ID_WORK] | BigQuery dataset that will hold ETL housekeeping tables (ex: swap tablet, etc...)
    | --bigquery-dataset-id-omop [BIGQUERY_DATASET_ID_OMOP] | BigQuery dataset that will hold the final OMOP tables
    | --google-cloud-storage-bucket-uri [GOOGLE_CLOUD_STORAGE_BUCKET_URI] | Google Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files. (the uri has format 'gs://{bucket_name}/{bucket_path}')


BigQuery
========

There are 2 ways to [authenticate]((https://cloud.google.com/docs/authentication/getting-started with GCP:
* Use a [Service Account key file](https://cloud.google.com/docs/authentication/production) with **--google-credentials-file** cli option
* When developing or testing you can use [Application Default Credentials (ADC)](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login)

    [Install](https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version) the gcloud CLI!

    For example for windows run the folowing powershell script:
    ```powershell
    (New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
    & $env:Temp\GoogleCloudSDKInstaller.exe
    ```

    Authenticate:
    ```bash
    # login
    gcloud auth application-default login
    # set our project
    PROJECT_ID="our_omop_etl_project_id_on_GCP" #you need to change this
    gcloud config set project ${PROJECT_ID}
    ```

    More info can also be found in the [Python API for GCP authentication](https://googleapis.dev/python/google-api-core/1.19.1/auth.html#overview)

TODO:
========

* improve documentation
* cost: add cost_event_id_table column to know the FK table
* episode_event: add event_id_table column to know the FK table
* observation.observation_event_id: 
* generate ERA tables (drug_era, dose_era and condition_era)