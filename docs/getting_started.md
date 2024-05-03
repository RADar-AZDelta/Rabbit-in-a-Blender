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

see the [database engines](database-engines.md)

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

Running this --create-db command will imply that you need to import the vocabularies again. 

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

Importing vocabularies from [Athena](https://athena.ohdsi.org/vocabulary/list) ensures that the data transformed into the OMOP CDM adheres to standardized vocabularies. These vocabularies represent hierarchical relationships, define concept classes or categories, describe relationships, and more. This process ensures data consistency and interoperability.

Import the downloaded vocabulary zip file with the command below:

```bash
riab --import-vocabularies ./my_downloaded_vocabulary.zip
```

> **Warning**: This command is very resource intensive on the computer running the riab command. Lower the max_parallel_tables value in your riab.ini file, when running into resource problems (like out of memory errors).


## 7. Create the CDM folder structure

RiaB uses a strict folder structure, when running the ETL. With the --create-folders command, RiaB will create the folder structure for you, and populate it with example queries, Usagi CSV's and custom concept CSV's.

```bash
riab --create-folders ./OMOP_CDM
```    

## 8. Craft the ETL queries


## 9. Map the concepts

## 10. Run the ETL

The restriction on vocabularies and classes within each mapping domain serves the purpose of ensuring a more standardized mapping of concepts across different persons. This approach aims to mitigate the proliferation of varied concepts, promoting consistency and facilitating a cohesive representation of data.

## 11. Check the Data Quality
