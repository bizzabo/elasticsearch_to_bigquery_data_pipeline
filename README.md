<p align="center"><img width="350" src="img/bizzabo.svg"></p>

<br>
<br>
<p align="center">
  <img src="img/airflow.png" width="70" />
  <img src="img/elasticsearch.png" width="70" /> 
  <img src="img/dataflow.png" width="70" />
  <img src="img/beam.png" width="70" />
  <img src="img/bigquery.png" width="70" />
</p>

![Kotlin](https://img.shields.io/badge/Kotlin-1.3-green.svg)
![Python](https://img.shields.io/badge/python-v3-green.svg)

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-blue.svg)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-blue.svg)
![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud_Dataflow-blue.svg)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-blue.svg)
![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-blue.svg)

![License](https://img.shields.io/badge/license-MIT-yellow.svg)

# Elasticsearch to BigQuery Data pipeline
#### A generic data pipeline which maps Elasticsearch documents to Google BigQuery table rows using Apache Airflow and Google Cloud Dataflow

## About
This application was developed following the need for an ETL process which would do the following:
* Retrieve documents from Elasticsearch,
* Transform said documents, and 
* Write them as rows to BigQuery
The application runs on Dataflow and is triggered periodically by Airflow

## Getting Started
This repo performs the following steps:
1. Gather application arguments
2. Create data pipeline with the provided arguments
3. Generate a query according to the provided arguments
4. Create a BigQuery table reference
5. Create a BigQuery table field schema
6. Apply actions to the pipeline (Read documents from Elasticsearch, transform read documents to table rows, write rows to BigQuery)

## Prerequisites
In order to run this application there are a number of things to set up:
1. The application will attempt to connect to Elasticsearch and gauge the number of documents that will be processed. 
This requires Elasticsearch to be accessible to Dataflow.
If your elasticsearch cluster is behind a firewall, network modifications may be required to prevent the application from falling to access Elasticsearch and therefore falling altogether.
2. The application requires BigQuery to include a table with the correct name and schema as defined in the setFields function.
If said table does not exist, writing to BigQuery will fail.

## Running
### CLI or IDE Execution

When the application is executed, a job is created in Dataflow and the application is run with the provided arguments.
Monitoring of the job can be done via Dataflow's web console.  
### Airflow Execution
When the relevant DAG is triggered, the application jar is executed along with any arguments provided by the DAG.
Monitoring of the job can be done via Dataflow's web console or via Airflow's web console.

## Arguments
The application's arguments can be divided into three categories:
### Pipeline Arguments
* queryType - determines which type of query will be used to retrieve documents from Elasticsearch.
Possible values: 
    * daysAgo - query will return documents modified between "daysBeforeStart" and "daysBeforeEnd".
    * betweenDates - query will return documents modified between "beginDate" and "endDate".
    * withSearchParam - query will return all of the documents in Elasticsearch which meet the criteria specified by "paramName" and "paramValue"
    * everything - query will return all of the documents in Elasticsearch
* beginDate - a YYYYMMDD formatted string that determines the bottom boundary for when the document was modified.
* endDate - a YYYYMMDD formatted string that determines the top boundary for when the document was modified.
* daysBeforeStart - an int value that determines the bottom boundary for how many days ago the document was modified.
* daysBeforeEnd - an int value that determines the top boundary for how many days ago the document was modified.
* paramName - the name of the parameter to be used as a criteria in the query.
* paramValue - the value of the parameter to be used as a criteria in the query.

### Elasticsearch Arguments
* batchSize - the Elasticsearch result batch size.
* connectTimeout - the Elasticsearch connection timeout duration.
* index - the Elasticsearch index to be queried against.
* socketAndRetryTimeout - the Elasticsearch socket and retry timeout duration.
* source - the url and port of the Elasticsearch instance to be queried against.
* type - the Elasticsearch document type.

### Google Cloud Arguments
* datasetId - BigQuery dataset ID.
* diskSizeGb - Dataflow worker disk size in GB.
* enableCloudDebugger - boolean indicator of whether to enable Cloud Debugger.
* gcpTempLocation - Dataflow temporary file storage location.
* network - Google Cloud VPC network name.
* numWorkers - number of Dataflow workers.
* project - Google Cloud Platform project name.
* projectId - Google Cloud Platform project ID.
* region - Google Cloud Platform VPC network region.
* serviceAccount - Google Cloud Platform service account.
* subnetwork - Google Cloud Platform VPC subnetwork.
* tableId - BigQuery table ID.
* tempLocation - Dataflow pipeline temporary file storage location.
* usePublicIps - boolean indicator of whether Dataflow should use public IP addresses.

Note: any argument which is not passed to the application will be replaced with a default value.

## Airflow Options
All of the arguments available to the application may be set by Airflow. There are a number of additional options available for Airflow:
* autoscalingAlgorithm - Dataflow autoscaling algorithm.
* partitionType - Dataflow partition type.

## Deployment
In order to deploy the application, it must be built into a fat jar so any dependencies are accessible to Dataflow during runtime.
If you plan on running the application using Airflow, the jar must be uploaded to an accessible location in Google Cloud Storage.

## Built With
The application is built with Gradle.

## Project Status
The project is currently in production and is run periodically as part of Bizzabo's data pipeline.
  
## License
This project is licensed under the MIT License - see the LICENSE.md file for details.
