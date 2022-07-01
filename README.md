![Rabbit in a Blender logo](resources/img/rabbitinablenderlogo.png)
===========

Introduction
========
**Rabbit in a Blender** is an ETL pipeline to transform your EMP data to OMOP.


BigQuery
========

[Install](https://cloud.google.com/sdk/docs/install-sdk#installing_the_latest_version) the gcloud CLI!

For example for windows run the folowing powershell script:
```powershell
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
& $env:Temp\GoogleCloudSDKInstaller.exe
```

[Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started)


If you’re developing locally, the easiest way to authenticate is [using the Google Cloud SDK](https://googleapis.dev/python/google-api-core/1.19.1/auth.html#overview):
```bash
# login
gcloud auth application-default login
# set our project
gcloud config set project ${PROJECT_ID}
```



TODO:
-----

* fact_relationship: add fact_id_1_table and fact_id_2_table column to know the FK tables
* cost: add cost_event_id_table column to know the FK table
* episode_event: add event_id_table column to know the FK table
* generate ERA tables (drug_era, dose_era and condition_era)