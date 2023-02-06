![Rabbit in a Blender](resources/img/rabbitinablenderlogo.png)
===========

**Rabbit in a Blender** is an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) pipeline [CLI](https://nl.wikipedia.org/wiki/Command-line-interface) to transform your [EMR](https://en.wikipedia.org/wiki/Electronic_health_record) data to [OMOP](https://www.ohdsi.org/data-standardization/the-common-data-model/).

Why the name 'Rabbit in a Blender'? It stays in the rabbit theme of the [OHDSI](https://www.ohdsi.org) tools, and an ETL pipeline is like putting all your data in a blender. 

No rabbits were harmed during the development of this tool!

Introduction
============

Extract-Transform-Load (ETL) processes are very complex and are mainly crafted by highly skilled data engineers. The process of transforming the electronic medical record (EMR) data into the observational medical outcomes partnership (OMOP) common data model (CDM) is no exception. The mapping process of the source values to standard concepts is mostly done by subject matter experts, who lack the knowledge of programming the ETL process. Wouldn’t it be nice if we could drastically simplify the ETL process, so that you don’t need seasoned data engineers to start the OMOP CDM journey. Imagine that you just save your queries, Usagi comma separated value (CSV) text files and custom concept CSV’s on disk, and run a command line interface (CLI) tool that does all the ETL magic automatically. 


Concept
=======

The main strength of the CDM is its simplified scheme. This scheme is a relational data model, where each table has a primary key and can have foreign keys to other tables. Because of the relational data model, we can extract the dependencies of the tables from the scheme. For example, the provider table is dependent on the care_site table, which is in its turn dependent on the location table. If we flatten that dependency graph, we have a sequence of ETL steps that we need to follow to have consistent data in our OMOP CDM. These ETL steps can be automated, so a hospital can focus its resources on the queries and the mapping of the concepts. The automated ETL consists of multiple tasks. It needs to execute queries, add custom concepts, apply the Usagi source to concept mapping, and do a lot of housekeeping. An example of that housekeeping is the autonumbering of the OMOP CDM primary keys, for which the ETL process needs to maintain a swap table that holds the key of the source table and the generated sequential number of the CDM table’s primary key. Another example of the housekeeping is the upload and processing of the Usagi CSV’s and also the upload and parsing of the custom concept CSV’s. In an ETL process data is divided in zones. The raw zone holds the source data (for example the data from the EMR), the work zone holds all the house keeping tables of the ETL process and the gold zone holds our final OMOP CDM.
After designing the architecture, the implementation needs to be developed. We have two options to choose from: configuration and convention as design paradigm. We choose convention over configuration, because it decreases the number of decisions the user has to make and eliminates the complexity. As convention a specific folder structure is adopted (see [our mappings as example](https://github.com/RADar-AZDelta/AZDelta-OMOP-CDM)). A folder is created for each OMOP CDM table, where the SQL queries are stored to fill up the specific CDM table. In the table folders we also have for each concept column a sub folder. Those concept column sub folders hold our Usagi CSV’s (files ending with _usagi.csv). We also have a custom folder in the concept column sub folder, that holds the custom concept CSV’s (files ending with _concept.csv). With this convention in place, our ETL CLI tool has everything it needs to do its magic.
One final requirement we want to build in the ETL CLI tool, is that each ETL step is an atomic operation, it either fails or succeeds, so that there is no possibility to corrupt the final OMOP CDM data.


Deviations from v5.4
====================

|Table|Change|Reason|
|---|---|---|
|fact_relationship|Renamed domain_concept_id_1 and _2 to field_concept_id_1 and _2|To stay consistent with similar fields like episode_event_field_concept_id. This follows Proposal 1 in the open issue [#230](https://github.com/OHDSI/CommonDataModel/issues/230).| 
|cost|Changed cost_domain_id with type STRING to cost_field_concept_id of type INT64|To stay consistent with similar fields like episode_event_field_concept_id. This follows the changes made in [v6.0](https://ohdsi.github.io/CommonDataModel/cdm60.html#COST)|
|source_id_to_omop_id_map|Additional table|The table holds de mapping between the source id's and the generated OMOP id's|


Notes on Use
============

You will need to run the cleanup command when concept mappings change in your existing Usagi CSV's. The cleanup is not necessary when you add new queries or add additional Usagi mappings.

The measurement table has the **measurement_event_id** field, the observation table has the **observation_event_id** field, the cost table has the **cost_event_id** field, the episode_event table has the **event_id** field and the fact_relationship table has the **fact_id_1** and **fact_id_2** fields. All those fields are foreign keys to almost all OMOP CMD tables. Put the source id in the event_id field and the reffered table in the field_concept_id field. An example of how to implement this in the sql-file:
- Linking two people with a personal relationship in the fact_relationship table:
```sql
select distinct
  'person' as field_concept_id_1  -- foreign table name as string
  ,pr.person_nr_1 as fact_id_1	-- same key as used as when adding the person to the person table
  ,'person' as field_concept_id_2  -- foreign table name as string	
  ,pr.person_nr_1 as fact_id_2	-- same key as used as when adding the person to the person table
  ,pr.relationship as relationship_concept_id  -- column with sourceCodes specifying the relationship and mapped in a _usagi.csv file in the relationship_concept_id folder
from person_relationships pr
```

The **custom concepts** get added to the **source_to_concept_map** table. The concept_code is used as the source_code, the newly assigned (>2.000.000.000) concept_id is used as the target_concept_id. The custom concepts are also added to the **usagi table**, using the concept_code as sourceCode and the newly assigned concept_id as the conceptId. The custom concepts can be used **as mapping targets** in two ways:
- No explicit mapping in usagi table: the source code (as assigned in your ETL sql file) is mapped to the custom concept where it equals the concept_code
- Explicit mapping in usagi: map a sourceCode in the usagi table to a custom concept by setting its targetConceptId equal to the concept_code of the custom concept.

Examples:

* mapping "Cemiplimab", with source_code *M1*:
    - Add custom concept for cemiplimab with concept_code = *M1*, it gets a assigned a concept_id automatically, no usagi-entry required
    - Optionally add an usagi-entry for clarity, mapping sourceCode = *M1* to targetConceptId = *M1*, the concept_id gets filled in automatically
* mapping "Atezolizumab + Cemiplimab", with source_code *C12*:
    - Add custom concept for cemiplimab with concept_code = *M1*, it gets a assigned a concept_id automatically
    - Add two usagi-entries: mapping sourceCode *C12* to targetConceptId = *1792776* (standard code for atezolizumab) and to targetConceptId = *M1*
    
    ALTERNATIVELY:

    - Add custom concept for cemiplimab with concept_code = *C12*, it gets a assigned a concept_id automatically, no usagi-entry required
    - Add only one usagi-entry, mapping sourceCode *C12* to targetConceptId = *1792776* (standard code for atezolizumab), the second mapping of *C12* to custom concept with concept_code = *C12* is done automatically.

For the moment we only implemented a BigQuery backend for the ETL process, because this is what our hospital uses. Other database technologies as ETL backend can be implemented.


Installation
========

```bash
pip install Rabbit-in-a-Blender
```

CLI Usage
========

* **Options**:
    |  command | help  
    |---|---
    | -h, --help | Show help message and exit
    | -v, --verbose | Verbose logging (logs are also writen to a log file in the systems tmp folder)

* **Required named arguments:**
    |  command | help  
    |---|---
    | -d [DB-ENGINE], --db-engine [DB-ENGINE] | The database engine technology the ETL is running on. Each database engine has its own legacy SQL dialect, so the generated ETL queries can be different for each database engine. For the moment only BigQuery is supported, yet 'Rabbit in a Blender' has an open design, so in the future other database engines can be added easily.


* **ETL Commands**:
    |  command | help  
    |---|---    
    | -cd, --create-db | Create the OMOP CDM tables
    | -cf, --create-folders [PATH] | Create the ETL folder structure that will hold your queries, Usagi CSV's an custom concept CSV's.
    | -i, --import-vocabularies [VOCABULARIES_ZIP_FILE] | Extracts the vocabulary zip file (downloaded from the Athena website) and imports it into the OMOP CDM database.
    | -r [PATH], --run-etl [PATH] | Runs the ETL, pass the path to ETL folder structure that holds your queries, Usagi CSV's an custom concept CSV's.
    | -c, --cleanup [TABLE] | Cleanup all the OMOP tables, or just one. Be aware that the cleanup of a single table can screw up foreign keys! For instance cleaning up only the 'Person' table, will result in clicical results being mapped to the wrong persons!!!!
    | -dq, --data-quality | Check the data quality and store the results.
    | -dqd, --data-quality-dashnoard | View the results of the data quality checks. (UNDER DEVELOPMENT)

* **Run ETL specific command options (-r [PATH], --run-etl [PATH]):**
    |  command | help  
    |---|---  
    | -t [TABLE], --table [TABLE] | Do only ETL on this specific OMOP CDM table
    | -s, --skip-usagi-and-custom-concept-upload | Skips the parsing and uploading of the Usagi and custom concept CSV's. Skipping results in a significant speed boost.

* **Data quality specific command options (-dq, --data-quality):**
    |  command | help  
    |---|---  
    | --json [PATH] | Save the data quality result as [JSON file](https://ohdsi.github.io/DataQualityDashboard/articles/DataQualityDashboard.html#viewing-results) for use in the OHDSI [Data Quality Dashboard](https://ohdsi.github.io/DataQualityDashboard/).

* **Data quality dashboard specific command options (-dqd, --data-quality-check):**
    |  command | help  
    |---|---  
    | --port [PORT] | The port the dashboard schould listen on.

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

CLI Examples
========

Create the OMOP CDM database:
```bash
riab --create-db \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop
```

Import your downloaded vocabularies (from [Athena](https://athena.ohdsi.org/vocabulary/list)) zip file:
```bash
riab --import-vocabularies ./vocabulary-2022-07-28.zip \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Create the ETL folder structure:
```bash
riab --create-folders ./OMOP_CDM \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop
```     

Run full ETL:
```bash
riab --run-etl ./OMOP-CDM \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --bigquery-dataset-id-raw emr \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Run ETL on one table:
```bash
riab --run-etl ./OMOP-CDM \
  --table provider \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --bigquery-dataset-id-raw emr \
    --google-cloud-storage-bucket-uri gs://omop/upload

```

Run ETL withour re-upload of Usagi CSV's and custom concept CSV's:
```bash
riab --run-etl ./OMOP-CDM \
  --skip-usagi-and-custom-concept-upload \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --bigquery-dataset-id-raw emr \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Cleanup all tables:
```bash
riab --cleanup \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Cleanup one table (example provider table):
```bash
riab --cleanup provider \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --bigquery-dataset-id-work omop_work \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Data quality check:
```bash
riab --data-quality \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --google-cloud-storage-bucket-uri gs://omop/upload
```

Data quality check (export result to JSON):
```bash
riab --data-quality \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
    --google-cloud-storage-bucket-uri gs://omop/upload \
  --json dqd_result.json
```

Data quality dashboard (default port = 8050):
```bash
riab --data-quality \
  --db-engine BigQuery \
    --bigquery-dataset-id-omop omop \
  --port 8888
```


BigQuery
========

There are 2 ways to [authenticate]((https://cloud.google.com/docs/authentication/getting-started) with GCP:
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


TODO
========

* improve documentation
* fix visits and observation period to be data driven (based on other clinical tables in OMOP)
* add concept_relationship and concept_ancestor tables for custom concepts to framework
* generate ERA tables (drug_era, dose_era and condition_era)
* support multiple GCS projects to split up raw, work and omop data

Authors
========

* [Lammertyn Pieter-Jan](https://github.com/pjlammertyn)
* [Dupulthys Stijn](https://github.com/stijndupulthys)
* [De Jaeger Peter](https://github.com/peterdejaeger)

License
========

Copyright © 2022, [RADar-AZDelta](mailto:radar@azdelta.be).
Released under the [GNU General Public License v3.0](LICENSE).

***
