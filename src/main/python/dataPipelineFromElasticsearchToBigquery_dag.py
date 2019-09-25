from datetime import timedelta

from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowJavaOperator
from airflow.operators.dummy_operator import DummyOperator
from dataPipelineFromElasticsearchToBigquery_options import options


def merge_dicts(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


all_options = merge_dicts(
    options['run_options'],
    options['pipeline_options'],
    options['gcp_options'],
    options['es_options']
)

dag_args = {
    'owner': 'Zohar',
    'depends_on_past': False,
    'start_date':
        datetime(2019, 9, 01),
    'email': ['dataPipelineFromElasticsearchToBigquery@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'dataflow_default_options': {
        'project': 'gcpProject',
        'zone': 'gpcNetworkZone',
        'stagingLocation': 'gs://dataPipelineFromElasticsearchToBigquery/airflowStaging/'
    }
}

dag = DAG('dataPipelineFromElasticsearchToBigquery-dag', default_args=dag_args, catchup=False)

start = DummyOperator(task_id='start', dag=dag)

task = DataFlowJavaOperator(
    task_id='daily-dataPipelineFromElasticsearchToBigquery-task',
    jar='gs://dataPipelineFromElasticsearchToBigquery/lib/dataPipelineFromElasticsearchToBigquery.jar',
    options=all_options,
    dag=dag)

start >> task
