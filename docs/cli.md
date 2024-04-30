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
    | -dqd, --data-quality-dashnoard | View the results of the data quality checks.
    | --print-etl-flow | Print the sequence in which the ETL tables that will be processed

* **Run ETL specific command options (-r [PATH], --run-etl [PATH]):**
    | command | help  
    |---|---  
    | -t [TABLE], --table [TABLE] | Do only ETL on this specific OMOP CDM table (this argument can be used multiple times). (ex: --run-etl ~/git/omop-cdm/ -t cdm_source -t metadata -t vocabulary -t location). This option is only usefull while developing the ETL queries or testing Usagi mappings, to speed up the ETL process. Do not use in production.
    | -q [PATH], --only-query [PATH] | Do ETL for a specified sql file in the CDM folder structure (this argument can be used multiple times). (ex: measurement/lab_measurements.sql). This option is only usefull while developing a specific ETL query, to speed up the ETL process. Do not use in production.
    | -s, --skip-usagi-and-custom-concept-upload | Skips the parsing and uploading of the Usagi and custom concept CSV's. Skipping results in a significant speed boost.
    | -sa, --process-semi-approved-mappings | In addition to 'APPROVED' as mapping status, 'SEMI-APPROVED' will be processed as valid Usagi concept mappings.
    | -se, --skip-event-fks-step | Skip the event foreign keys ETL step.

* **Data quality specific command options (-dq, --data-quality):**
    | command | help  
    |---|---  
    | --json [PATH] | Save the data quality result as [JSON file](https://ohdsi.github.io/DataQualityDashboard/articles/DataQualityDashboard.html#viewing-results) for use in the OHDSI [Data Quality Dashboard](https://ohdsi.github.io/DataQualityDashboard/).

* **Data quality dashboard specific command options (-dqd, --data-quality-check):**
    |  command | help  
    |---|---  
    | --port [PORT] | The port the dashboard schould listen on.