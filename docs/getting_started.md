# Getting started with RiaB

1. [Installing RiaB](#1-installing-riab)
2. [Configure the database](#2-configure-the-database)
3. [Create the riab.ini file](#3-create-the-riabini-file)
4. [Test database connection](#4-test-database-connection)
5. [Create the database](#5-create-the-database)
6. [Download and import the vocabularies](#6-download-and-import-the-vocabularies)
7. [Create the CDM folder structure](#7-create-the-cdm-folder-structure)
8. [Craft the ETL queries](#8-craft-the-etl-queries)
9. [Map the concepts](#9-map-the-concepts)
10. [Run the ETL](#10-run-the-etl)
11. [Check the Data Quality](#11-check-the-data-quality)

> **Tip**: adding the --verbose flag argument to the command line, will enable verbose logging output. 

## 1. Installing RiaB

see the [installation](installation.md)

## 2. Configure the database

see the [database engines](database_engines.md) 

## 3. Create the riab.ini file

see the [config](config.md)

## 4. Test database connection

To verify that your riab.ini is configured correctly, you can run the --test-db-connection command:

```bash
riab --test-db-connection
```

## 5. Create the database

Create the OMOP CDM tables by running the --create-db command:

```bash
riab --create-db
```

Running this --create-db command will imply that you need to import the vocabularies again, it also implies that it removes everything in the OMOP dataset. 

## 6. Download and import the vocabularies

Select and download the vocabulary zip file from the [Athena](https://athena.ohdsi.org/vocabulary/list) website. Select internationally recognized vocabularies, ensuring a global perspective. This import will populate the following tables:  "concept",
            "concept_ancestor",
            "concept_class",
            "concept_relationship",
            "concept_synonym",
            "domain",
            "drug_strength",
            "relationship" and
            "vocabulary".

When vocabularies are downloaded from Athena, concepts of the CPT4 vocabulary are not present in the CONCEPT.csv, while present in the other vocabulary csv's. Check [steps](post-process-CPT4-concepts.md) to overcome.

Importing vocabularies from [Athena](https://athena.ohdsi.org/vocabulary/list) ensures that the data transformed into the OMOP CDM adheres to standardized vocabularies. These vocabularies represent hierarchical relationships, define concept classes or categories, describe relationships, and more. This process ensures data consistency and interoperability.

Import the downloaded vocabulary zip file with the command below:

```bash
riab --import-vocabularies ./my_downloaded_vocabulary.zip
```

> **Warning**: This command is very resource intensive on the computer running the riab command. Lower the max_parallel_tables value in your riab.ini file, when running into resource problems (like out of memory errors).


## 7. Create the CDM folder structure

RiaB uses a strict folder structure, when running the ETL. With the --create-folders command, RiaB will create the folder structure for you, and populate it with example queries, Usagi CSV's and custom concept CSV's. The OMOP folders contain source queries. In the OMOP table folders are concept_id subfolders. They contain the mappings (usagi_csv files), the queries to generate the input csv files for the mapping tool and de csv files themselves as input for the mapping tool. 

```bash
riab --create-folders ./OMOP_CDM
```    
It is important to have the necessary subfolders 

## 8. Craft the ETL queries

In order to extract data from your source databases or application, ETL queries need to be constructed. The raw zone will hold these source data.

The fields in the select query, are the column names from the CDM table

Assign primary keys to tables in the CDM to uniquely identify each record. Primary keys should be unique for efficient querying and indexing.
Establish foreign key relationships between tables in the CDM to maintain data integrity and enable relational querying. 
Apply necessary data transformations, such as standardizing date formats, handling missing or null values, and aggregating or disaggregating data as needed to fit the CDM structure.
Ensure consistency in data types between source data and the OMOP CDM. This involves mapping source data types to corresponding CDM data types and ensuring appropriate data type lengths to accommodate data values.
Ensure adherence to ETL conventions, such as maintaining appropriate data type lengths for fields in the Common Data Model.

Tips:

-Use the strict folder structure

-Keep your ETL queries as simple as possible 

-Use prequels on id and concept_id columns to make sure there are no duplicates (primary keys, foreign keys and concept columns)


![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/0724d2e9-9913-4574-a87b-00c0818db201)


## 9. Map the concepts

Queries are used to generate input CSV files for a mapping tool. Use [Keun](https://radar-azdelta.github.io/Keun/) or Usagi for the mapping and store the output after the mapping as _usagi.csv in your correct subfolder. Store custom concepts as _concept.csv in the correct custom subfolders

### 9.1 Mappings

Load codes from your sources system (“source codes”) that you would like to map to OMOP Vocabulary concepts. 
Usagi will run term similarity approach to map source codes to OMOP Vocabulary concepts.
Leverage Usagi interface to check suggested mappings or create maps. Preferably an individual who has experience with the coding system and medical terminology should be used for this review.
Export final map generated by Usagi into the OMOP Vocabulary’s SOURCE_TO_CONCEPT_MAP.
Base the ‘Usagi source CSV’ query on the ETL query

### 9.2 Custom mappings
Make sure your custom mappings are in a custom subfolder and have an empty first concept_id column since RiaB will create an appropriate integer for the concept_id column

## 10. Run the ETL

Run the RiaB ETL via the [ETL Commands](cli.md). If you run RiaB for the first time, you can run the ETL on one or several tables. Use the ETL flow structure for v5.4. so you don't have foreign keys dependencies errors.

Make sure you add a verbose flag "-v" to get more detailed output during execution of the ETL runs.

The restriction on vocabularies and classes within each mapping domain serves the purpose of ensuring a more standardized mapping of concepts across different persons. This approach aims to mitigate the proliferation of varied concepts, promoting consistency and facilitating a cohesive representation of data.

If mappings [9] or ETL queries [10] have changed in your existing usagi_csv files, you first need to do a cleanup of the appropriate OMOP tables. A cleanup command will cleanup all work tables and remove the custom concepts. Make sure you follow the correct table order to avoid foreign key issues when doing a RiaB re-run.

## 11. Check the Data Quality


