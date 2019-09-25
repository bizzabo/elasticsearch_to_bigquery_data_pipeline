options = {
    'run_options': {
        'daysBeforeStart': '1',
        'daysBeforeEnd': '0',
        'paramName': 'attributes.paramName.raw',
        'paramValue': 'Zohar',
        'queryType': 'betweenDates',
        'beginDate': '{{ yesterday_ds_nodash }}',
        'endDate': '{{ ds_nodash }}'
    },
    'pipeline_options': {
        'autoscalingAlgorithm': 'BASIC',
        'partitionType': 'DAY',
        'project': 'gcpProject',
        'tempLocation': 'gs://dataPipelineFromElasticsearchToBigquery/tempLocation/',
        'gcpTempLocation': 'gs://dataPipelineFromElasticsearchToBigquery/gcpTempLocation/',
        'serviceAccount': 'service@account.iam.gserviceaccount.com',
        'region': 'gcpRegion',
        'network': 'gcpNetwork',
        'subnetwork': 'regions/gcpRegion/subnetworks/subNetwork',
        'usePublicIps': 'false',
        'numWorkers': '1000',
        'diskSizeGb': '100',
        'enableCloudDebugger': 'true'
    },
    'gcp_options': {
        'datasetId': 'datasetId',
        'projectId': 'gcpProjectId',
        'tableId': 'table'
    },
    'es_options': {
        'connectTimeout': '5000',
        'index': 'elasticsearchIndex',
        'source': 'http://elasticsearch.data.source.com:9200',
        'type': 'documentType',
        'socketAndRetryTimeout': '90000',
        'batchSize': '5000'
    }
}
