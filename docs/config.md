# riab.ini config

RiaB searches for the ini config file by using the following cascade:
1. CLI --config argument
2. RIAB_CONFIG environment variable (the RIAB_CONFIG environment variable can also be placed in a .env file in the current folder)
3. riab.ini in the current folder

Below an example of a config:

```ini
[riab]
db_engine=bigquery
; Required
; What database are you using? (bigquery or sql_server)
max_parallel_tables=9
; Optional
; The number of tables, that RiaB will process in parallel. On a server with a performant db_engine (like BigQuery), this number can be high. On slower machines/database set this to a low number to avoid overwhelming the database or server. (if you have problems importing the vocabularies, try lowering this number to 1 or 2)
; The default value is 9
max_worker_threads_per_table=16
; Optional
; The number of worker threads that RiaB will use, per table, to run stuff in parallel. On a server with a performant db_engine (like BigQuery), this number can be high. On slower machines/database set this to a low number to avoid overwhelming the database or server.
; The default value is 16

[bigquery]
credentials_file=service_account.json
; Optional
; The credentials file must be a service account key, stored authorized user credentials, external account credentials, or impersonated service account credentials. (see https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.load_credentials_from_file)
; Alternatively, you can also use 'Application Default Credentials' (ADC) (see https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login)
location=EU
; Location where to run the BigQuery jobs. Must match the location of the datasets used in the query. (important for GDPR)
project_raw=my_omop_project
; Optional
; Can be handy if you use jinja templates for your ETL queries (ex if you are using development-staging-production environments). Must have the following format: PROJECT_ID
dataset_work=my_omop_project.work
; The dataset that will hold RiaB's housekeeping tables. Must have the following format: PROJECT_ID.DATASET_ID
dataset_omop=my_omop_project.omop
; The dataset that will hold the OMOP tables. Must have the following format: PROJECT_ID.DATASET_ID
dataset_dqd=my_omop_project.dqd
; The dataset that will hold the data quality tables. Must have the following format: PROJECT_ID.DATASET_ID
dataset_achilles=my_omop_project.achilles
; The dataset that will hold the data achilles tables. Must have the following format: PROJECT_ID.DATASET_ID
bucket=gs://my_omop_bucket/upload
; The Cloud Storage bucket uri, that will hold the uploaded Usagi and custom concept files. (the uri has format 'gs://{bucket_name}/{bucket_path}')

[sql_server]
server=127.0.0.1
; The SQL Server host
port=1433
; The SQL Server port (defaults to 1433)
user=riab
; The SQL Server user
password=?????
; The SQL Server password
omop_database_catalog=omop
; The SQL Server database catalog that holds the OMOP tables
omop_database_schema=dbo
; The SQL Server database schema that holds the OMOP tables
work_database_catalog=work
; The SQL Server database catalog that holds the RiaB's housekeeping tables
work_database_schema=dbo
; The SQL Server database schema that holds the RiaB's housekeeping tables
dqd_database_catalog=dqd
; The SQL Server database catalog that holds the data quality tables
dqd_database_schema=dbo
; The SQL Server database schema that holds the data quality tables
achilles_database_catalog=achilles
; The SQL Server database catalog that holds the data achilles tables
achilles_database_schema=dbo
; The SQL Server database schema that holds the data achilles tables
raw_database_catalog=raw
; Optional
; The SQL Server database catalog that holds the raw tables
raw_database_schema=dbo
; Optional
; The SQL Server database schema that holds the raw tables
; Changing this flag requires that you re-run the following commands: --create-db, --cleanup and --import-vocabularies
; Default value is false
; Set to false for better data quality
disable_fk_constraints=false
; Optional
; By default foreign key constraints are disabled, because they are very resource consuming. (true or false are allowes as value)
bcp_code_page=ACP
; Optional
; Default value is ACP. For more info see https://learn.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver16#-c--acp--oem--raw--code_page-
```
