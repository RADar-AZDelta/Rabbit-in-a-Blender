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
It is important to have the necessary subfolders in your folder structure. 

The entire folder structure should look like this:

<img width="295" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/9041e75b-4756-400c-b114-2a6eaf33c9be">

> **Tip**: Place your riab.ini file on the same level as your table folders!

An example of such a folder structure for the OMOP table visit_occurrence: 

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/1d3e86c6-96cf-4e24-ba00-cb04512c21e6)


## 8. Craft the ETL queries

In order to extract data from your source databases or clinical applications, ETL queries need to be constructed. RiaB will transfer source data towards the raw zone.

The fields in the select of your ETL query, are the column names from each OMOP CDM table
Assign primary keys to tables in the CDM to uniquely identify each record. Primary keys should be unique for efficient querying and indexing.
Establish foreign key relationships between tables in the CDM to maintain data integrity and enable relational querying. 
Apply necessary data transformations, such as standardizing date formats, handling missing or null values, and aggregating or disaggregating data as needed to fit the CDM structure.
Ensure consistency in data types between source data and the OMOP CDM. This involves mapping source data types to corresponding CDM data types and ensuring appropriate data type lengths to accommodate data values.
Ensure adherence to ETL conventions, such as maintaining appropriate data type lengths for fields in the CDM.

Tips:

-Use the strict folder structure

-Keep your ETL queries as simple as possible (select, from, where)

-Use prequels on id and concept_id columns to make sure there are no duplicates (primary keys, foreign keys and concept columns)

-Check the ETL conventions and user guide on [OHDSI github OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html#observation)

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/0724d2e9-9913-4574-a87b-00c0818db201)


## 9. Map the concepts

'Usagi source CSV' queries are used to generate input CSV files for a mapping tool. Base the ‘Usagi source CSV’ query on the ETL query. Select sourceCode, sourceName and sourceFrequency as input csv file. Use [Keun](https://radar-azdelta.github.io/Keun/) or [Usagi](https://ohdsi.github.io/Usagi/) for the mapping and store the output after finalizing the mapping as _usagi.csv in your correct subfolder in the folder structure. Store custom concepts with sequel _concept.csv in the correct custom subfolders. Your mappings should contain the column headers as a first row so that RiaB knows where e.g. the conceptId is situated.

```
└──table_name_folder
  └──relationship_concept_id --folder containing input files for mapping, mapping files, custom concept filees
  |  ├──custom
  |  |  └──xxx_concept.csv  -- custom concept file     
  |  ├──xxx.csv             -- input csv for mapping
  |  ├──xxx_usagi.csv       -- csv file containing the mappings
  |  └──xxx.sql             -- query to create your input csv for mapping
  └──xxx.sql               -- etl query
```

For more information, see [mappings](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/edit/main/docs/mappings.md)


## 10. Run the ETL

Run the RiaB ETL via the [ETL Commands](cli.md). If you run RiaB for the first time, you can run the ETL on one or several tables. Use the ETL flow structure for v5.4. so you don't have foreign keys dependencies errors. With the --print-etl-flow command you can see the sequence in which the ETL tables will be processed.

Make sure you add a verbose flag "-v" to get more detailed output during execution of the ETL runs.

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/81d49b0b-1259-42c8-95a1-86ffd813011b)

Each RiaB run will create a log file.

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/8ff60b40-e02f-4f19-a003-e74042f8bd43)

The restriction on vocabularies and classes within each mapping domain serves the purpose of ensuring a more standardized mapping of concepts across different persons. This approach aims to mitigate the proliferation of varied concepts, promoting consistency and facilitating a cohesive representation of data.

If mappings [9] or ETL queries [8] have changed in your existing usagi_csv files, you first need to do a cleanup of the appropriate OMOP tables. A cleanup command will cleanup all work tables and remove the custom concepts. Make sure you follow the correct table order to avoid foreign key issues when doing a RiaB re-run.

RiaB will only handle an 'APPROVED' mapping status, you can add the --process-semi-approved-mappings command to process any 'SEMI-APPROVED' mappings.

It is possible to do an ETL for a specified sql file, skip parsing or uploading of the USAGI or custom concept csv's or skip event foreign keys.

> **Warning:** If you make changes to queries or Usagi and custom mapping CSV's, you xill need to run the --cleanup command!

## 11. Check the Data Quality

Checking data quality of your ETL and mapping is done via the command --data-quality. Using this framework, the Data Quality Dashboard takes a systematic-based approach to running data quality checks. View the results in a dashboard via --data-quality-dashboard, select a correct default port and click on the weblink. 

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/e7dea81e-00d3-41b7-afb4-5ab354c49bd7)

An export of the data quality results can be achieved in [JSON file](https://ohdsi.github.io/DataQualityDashboard/articles/DataQualityDashboard.html#viewing-results) format for use in the [OHDSI Data Quality Dashboard](https://ohdsi.github.io/DataQualityDashboard/).
