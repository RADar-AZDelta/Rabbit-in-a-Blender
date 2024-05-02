# riab.ini config

With the addition of additional database engines, we switched to a [ini](https://en.wikipedia.org/wiki/INI_file) config file for database specific configurations.
This makes the CLI arguments less cumbersome.

RiaB searches for the ini config file by using the following cascade:
1. CLI --config argument
2. RIAB_CONFIG environment variable (the RIAB_CONFIG environment variable can also be placed in a .env file in the current folder)
3. riab.ini in the current folder

* **riab** section:

    | key | description | required | default value
    |---|---|---|---   
    | db_engine | What database are you using? (bigquery or sql_server) | true |
    | max_parallel_tables | The number of tables, that RiaB will process in parallel. On a server with a performant db_engine (like BigQuery), this number can be high. On slower machines/database set this to a low number to avoid overwhelming the database or server. (if you have problems importing the vocabularies, try lowering this number to 1 or 2) | | 9
    | max_worker_threads_per_table | The number of worker threads that RiaB will use, per table, to run stuff in parallel. On a server with a performant db_engine (like BigQuery), this number can be high. On slower machines/database set this to a low number to avoid overwhelming the database or server. | | 16 

* **bigquery** section:

    | key | description | required | default value
    |---|---|---|---  
    | credentials_file | The [credentials file](https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.load_credentials_from_file) must be a service account key, stored authorized user credentials, external account credentials, or impersonated service account credentials.  Alternatively, you can also use [Application Default Credentials (ADC)](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login) | |
    | location | Where to run the BigQuery jobs. Must match the location of the datasets used in the query. (important for GDPR) | true | 
    | project_raw | TCan be handy if you use jinja templates for your ETL queries (ex if you are using development-staging-production environments). Must have the following format: PROJECT_ID | | 
    | dataset_work | The dataset that will hold RiaB's housekeeping tables. Must have the following format: PROJECT_ID.DATASET_ID | | work 
    | dataset_omop | The dataset that will hold the OMOP tables. Must have the following format: PROJECT_ID.DATASET_ID | | omop 
    | dataset_dqd | The dataset that will hold the data quality tables. Must have the following format: PROJECT_ID.DATASET_ID | | dqd 
    | dataset_achilles | The dataset that will hold the data achilles tables. Must have the following format: PROJECT_ID.DATASET_ID | | achilles 
    | bucket | The Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files. (the uri has format 'gs://{bucket_name}/{bucket_path}') | true |

* **sql_server** section:

    | key | description | required | default value
    |---|---|---|---  
    | server | The SQL Server host (IP or DNS name) | treu |
    | port | The SQL Server port | | 1433
    | user | The SQL Server user | true |
    | password | The SQL Server password | true |
    | raw_database_catalog | The SQL Server database catalog that holds the raw tables | | raw
    | raw_database_schema | The SQL Server database schema that holds the raw tables | | dbo
    | omop_database_catalog | The SQL Server database catalog that holds the OMOP tables | | omop
    | omop_database_schema | The SQL Server database schema that holds the OMOP tables | | dbo
    | work_database_catalog | The SQL Server database catalog that holds the RiaB's housekeeping tables | | work
    | work_database_schema | The SQL Server database schema that holds the RiaB's housekeeping tables | | dbo
    | dqd_database_catalog | The SQL Server database catalog that holds the data quality tables | | dqd
    | dqd_database_schema | The SQL Server database schema that holds the data quality tables | | dbo
    | achilles_database_catalog| The SQL Server database catalog that holds the data achilles tables | | achilles
    | achilles_database_schema | The SQL Server database schema that holds the data achilles tables | | dbo
    | disable_fk_constraints | Disable foreign key constraints. Changing this flag requires that you re-run the following commands: --create-db, --cleanup and --import-vocabularies! | | false
    | bcp_code_page | For more info see BCP [code page](https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16#-c--acp--oem--raw--code_page-) | | ACP


Example riab.ini for BigQuery:

```ini
[riab]
db_engine=bigquery
max_parallel_tables=9
max_worker_threads_per_table=16

[bigquery]
credentials_file=service_account.json
location=EU
project_raw=my_omop_project
dataset_work=my_omop_project.work
dataset_omop=my_omop_project.omop
dataset_dqd=my_omop_project.dqd
dataset_achilles=my_omop_project.achilles
bucket=gs://my_omop_bucket/upload
```

Example riab.ini for Sql Server:

```ini
[riab]
db_engine=sql_server
max_parallel_tables=9
max_worker_threads_per_table=16

[sql_server]
server=127.0.0.1
port=1433
user=riab
password=?????
omop_database_catalog=omop
omop_database_schema=dbo
work_database_catalog=work
work_database_schema=dbo
dqd_database_catalog=dqd
dqd_database_schema=dbo
achilles_database_catalog=achilles
achilles_database_schema=dbo
raw_database_catalog=raw
raw_database_schema=dbo
```

Example riab.ini for Azure Sql Server:

```ini
[riab]
db_engine=sql_server
max_parallel_tables=9
max_worker_threads_per_table=16

[sql_server]
server=my_omop.database.windows.net
port=1433
user=riab
password=?????
omop_database_catalog=riab
omop_database_schema=omop
work_database_catalog=riab
work_database_schema=work
dqd_database_catalog=riab
dqd_database_schema=dqd
achilles_database_catalog=riab
achilles_database_schema=achilles
raw_database_catalog=riab
raw_database_schema=raw
```
