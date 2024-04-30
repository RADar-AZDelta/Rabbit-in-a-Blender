# Database engines

Following database engines are supported:

- [BigQuery](#BigQuery)
- [Sql Server (on-premise and Azure)](#Sql-Server)

## BigQuery

There are 2 ways to [authenticate](https://cloud.google.com/docs/authentication/getting-started) with GCP:
* [Service Account key file](https://cloud.google.com/docs/authentication/provide-credentials-adc#local-key) with **--google-credentials-file** cli option
* [Application Default Credentials (ADC)](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login)

    ```bash
    # login
    gcloud auth application-default login
    # set our project
    PROJECT_ID="our_omop_etl_project_id_on_GCP" #you need to change this
    gcloud config set project ${PROJECT_ID}
    # you can alternatively set the project_id with a environment variable
    export GOOGLE_CLOUD_PROJECT=${PROJECT_ID}
    ```

    More info can also be found in the [Python API for GCP authentication](https://googleapis.dev/python/google-api-core/1.19.1/auth.html#overview)

The creation of different datasets in needed before config settings.

## Sql Server (on-premise and Azure)

> **Warning**: Only SQL Server 2017 or later are supported.

### SQL Server rights

The SQL user (configured in the riab.ini configuration file) requires the [db_ddladmin](https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/database-level-roles?view=sql-server-ver16) role (see line user=riab). Ask your Database Adminstrator (DBA), to create the user and grant the required rights.


```sql
CREATE USER riab WITH password='?????';
EXEC sp_addrolemember 'db_datareader', 'riab';
EXEC sp_addrolemember 'db_datawriter', 'riab';
EXEC sp_addrolemember 'db_ddladmin', 'riab';
```

### SQL Server databases and schemes

The creation of different database [schemas](https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/create-a-database-schema) or [databases](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-database-transact-sql) (work, omop, dqd, achilles) is needed.

> **Warning**: On Azure SQL Database, you need to use the **same database catalog** for omop, work, dqd and achilles. Because of the following limitation: To change database context to a different database in Azure SQL Database, you must create a new connection to that database. (see [T-SQL differences between SQL Server and Azure SQL Database](https://learn.microsoft.com/en-us/azure/azure-sql/database/transact-sql-tsql-differences-sql-server?view=azuresql)). So the omop_database_catalog, work_database_catalog, dqd_database_catalog and achilles_database_catalog must have the same value in the riab.ini!

```sql
CREATE DATABASE omop; 
--dbo is default schema for a new database
CREATE DATABASE work;
CREATE DATABASE dqd;
CREATE DATABASE achilles;
```

or

```sql
CREATE DATABASE omop;
USE omop;
CREATE SCHEMA omop;
CREATE SCHEMA work;
CREATE SCHEMA dqd;
CREATE SCHEMA achilles;
```

> Tip: It is best practices to avoid the usage of special characters (ex: the minus sign) in the name of the database catalog and schemes. 

### SQL Server recovery mode

Change the recovery mode to Simple. (ask your DBA for best practices)

```sql
ALTER DATABASE omop SET RECOVERY SIMPLE;
ALTER DATABASE work SET RECOVERY SIMPLE;
ALTER DATABASE dqd SET RECOVERY SIMPLE;
ALTER DATABASE achilles SET RECOVERY SIMPLE;
```

or

```sql
ALTER DATABASE omop SET RECOVERY SIMPLE;
```

**Tip**: Make sure you've chosen the right [collation](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-database-transact-sql?view=azuresqldb-current&preserve-view=true&tabs=sqlpool#collation_name), that is compatible with your raw data.

### Linked server to the raw data

If the raw EMR data is not on the same server defined in the riab.ini file, you will need to ask your database administrator, to add it as linked server.

Example:

```sql
USE master;  
GO  
EXEC sp_addlinkedserver   
   N'raw-emr-database-server,1433',  
   N'SQL Server';
GO

EXEC sp_addlinkedsrvlogin
   @rmtsrvname = N'raw-emr-database-server,1433',
   @useself = N'False',
   @locallogin = N'sa',
   @rmtuser = N'remote_user',
   @rmtpassword = N'???';
GO

sp_testlinkedserver N'raw-emr-database-server,1433';
```

### Azure SQL Server specific settings

- Ensure SQL allows non- Entra ID users:
   1. Open Azure portal
   2. Go to SQL **server** instance (not database)
   3. Under settings, make sure "Support only Microsoft Entra authentication for this server" is **NOT** checked.

- Scale up max vCores: You might need to [scale up](https://learn.microsoft.com/en-us/azure/azure-sql/database/scale-resources?view=azuresql) the number of max vCores to speed up for instance the import of the vocabularies. Especially with a high **max_parallel_tables** value, setting max vCores to 16 or more is recommended.

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/1187178/1ac67835-b467-4278-8c9a-171af0a98aa8)