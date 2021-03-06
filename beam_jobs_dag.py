from airflow import DAG
from datetime import datetime 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}
def helloworld():
    print('Hello Word')

with DAG("apche_beam", 
    start_date=datetime(2021, 1 ,1), 
    schedule_interval='@once', 
    catchup=False  # Catchup  
    ) as dag:

    t1 = PythonOperator(
        task_id = 'hello world',
        python_callable=helloworld
    )

    create_python_op = DataflowCreatePythonJobOperator(
        py_file = '/Users/luis.morales/Desktop/Test-env/movies_review.py',
        job_name = 'movies_review',
        gcp_conn_id = ''
    )

    movies_pipeline_dataflow_runner = BeamRunPythonPipelineOperator(
        task_id="movies_review_job",
        runner="DataflowRunner",
        py_file=GCS_PYTHON,
        pipeline_options={
            'tempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'output': GCS_OUTPUT,
        },
        py_options=[],
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False
    )

    logs_review_pipeline_dataflow_runner = BeamRunPythonPipelineOperator(
        task_id="logs_review_job",
        runner="DataflowRunner",
        py_file=GCS_PYTHON,
        pipeline_options={
            'tempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'output': GCS_OUTPUT,
        },
        py_options=[],
        py_requirements=['apache-beam[gcp]==2.26.0'],
        py_interpreter='python3',
        py_system_site_packages=False
    )

    t1 >> movies_pipeline_dataflow_runner >> logs_review_pipeline_dataflow_runner


    
