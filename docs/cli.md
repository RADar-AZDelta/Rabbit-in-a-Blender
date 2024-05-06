# CLI commands

* **Options**:
    | command | help  
    |---|---
    | -h, --help | Show help message and exit
    | -v, --verbose | Verbose logging (logs are also writen to a log file in the systems tmp folder)
    | -V, --version | The current installed version
    | --config | Optional path to the ini config file that holds the database engine configuration. Alternatively set the RIAB_CONFIG environment variable, pointing to the ini file. Or place a riab.ini file in the current directory.

* **ETL Commands**:
    | command | help  
    |---|---    
    | -tdc, --test-db-connection | Test the database connection
    | -cd, --create-db | Create the OMOP CDM tables
    | -cf, --create-folders [PATH] | Create the ETL folder structure that will hold your queries, Usagi CSV's an custom concept CSV's.
    | -i, --import-vocabularies [VOCABULARIES_ZIP_FILE] | Extracts the vocabulary zip file (downloaded from the Athena website) and imports it into the OMOP CDM database.
    | -r [PATH], --run-etl [PATH] | Runs the ETL, pass the path to ETL folder structure that holds your queries, Usagi CSV's an custom concept CSV's.
    | -c, --cleanup [TABLE] | Cleanup all the OMOP tables, or just one. Be aware that the cleanup of a single table can screw up foreign keys! For instance cleaning up only the 'Person' table, will result in clicical results being mapped to the wrong persons!!!!
    | -dq, --data-quality | Check the data quality and store the results.
    | -dqd, --data-quality-dashboard | View the results of the data quality checks.
    | --print-etl-flow | Print the sequence in which the ETL tables that will be processed

* **Run ETL specific command options (-r [PATH], --run-etl [PATH]):**
    | command | help  
    |---|---  
    | -t [TABLE], --table [TABLE] | Do only ETL on this specific OMOP CDM table (this argument can be used multiple times). (ex: --run-etl ~/git/omop-cdm/ -t cdm_source -t metadata -t vocabulary -t location). This option is only usefull while developing the ETL queries or testing Usagi mappings, to speed up the ETL process. Do not use in production.
    | -q [PATH], --only-query [PATH] | Do ETL for a specified sql file in the CDM folder structure (this argument can be used multiple times). (ex: measurement/lab_measurements.sql). This option is only usefull while developing a specific ETL query, to speed up the ETL process. Do not use in production.
    | -s, --skip-usagi-and-custom-concept-upload | Skips the parsing and uploading of the Usagi and custom concept CSV's. Skipping results in a significant speed boost.
    | -sa, --process-semi-approved-mappings | In addition to 'APPROVED' as mapping status, 'SEMI-APPROVED' will be processed as valid Usagi concept mappings.
    | -se, --skip-event-fks-step | Skip the event foreign keys ETL step.

* **Cleanup specific command options (-c [TABLE], --cleanup [TABLE]):**

    | command | help  
    |---|---  
    | --clear-auto-generated-custom-concept-ids | Cleanup the auto generated custom concept ID's (above 2 million). Without this argument, the cleanup command will not clear the mapping table (that maps the custom concept with the auto generated id above 2 million), so that you can use those above 2 million concept id's in your cohort builder, without the fear that those id's will change.

* **Data quality specific command options (-dq, --data-quality):**
    | command | help  
    |---|---  
    | --json [PATH] | Save the data quality result as [JSON file](https://ohdsi.github.io/DataQualityDashboard/articles/DataQualityDashboard.html#viewing-results) for use in the OHDSI [Data Quality Dashboard](https://ohdsi.github.io/DataQualityDashboard/).

* **Data quality dashboard specific command options (-dqd, --data-quality-check):**
    |  command | help  
    |---|---  
    | --port [PORT] | The port the dashboard schould listen on.


## CLI Examples

Create the OMOP CDM database:
```bash
riab --create-db
```

Import your downloaded vocabularies (from [Athena](https://athena.ohdsi.org/vocabulary/list)) zip file:
```bash
riab --import-vocabularies ./vocabulary_20240329.zip
```

Create the ETL folder structure:
```bash
riab --create-folders ./OMOP_CDM
```     

Run full ETL:
```bash
riab --run-etl ./OMOP-CDM
```

Run ETL on one table:
```bash
riab --run-etl ./OMOP-CDM \
  --table provider
```

Run ETL without re-upload of Usagi CSV's and custom concept CSV's:
```bash
riab --run-etl ./OMOP-CDM \
  --skip-usagi-and-custom-concept-upload
```

Run ETL for a specified sql file in the CDM folder structure. (ex: measurement/lab_measurements.sql)
```bash
riab --run-etl ./OMOP-CDM \
  --skip-usagi-and-custom-concept-upload \
  --skip-event-fks-step \
  --only-query measurement/lab_measurements.sql
```

Run ETL with SEMI-APPROVED concepts during ETL testing in dev branch
```bash
riab --run-etl ./OMOP-CDM \
  --process-semi-approved-mappings
```

Cleanup all tables:
```bash
riab --cleanup
```

Cleanup one table (example provider table):
```bash
riab --cleanup provider
```

Data quality check:
```bash
riab --data-quality
```

Data quality check (export result to JSON):
```bash
riab --data-quality
  --json dqd_result.json
```

Data quality dashboard (default port = 8050):
```bash
riab --data-quality-dashboard
  --port 8888
```
