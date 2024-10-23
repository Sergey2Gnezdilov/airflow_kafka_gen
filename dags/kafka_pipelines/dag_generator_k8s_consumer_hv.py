from datetime import timedelta
from os import path
import copy

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from plugins import yaml_tools
from plugins.k8s import k8s_configs
from plugins.telegram import failed_task_notification

config_path = path.join(path.dirname(__file__), f"{path.basename(__file__).replace('.py', '')}.yaml")
config_dags = yaml_tools.parse_config(config_path)
base_url = "https://airflow.co/home/"

def task_failure_alert(context):
    
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")

def task_retry_alert(context):
    print(f"Task has retried, task_instance_key_str: {context['task_instance_key_str']}")
    print(f"Retry! airflow task: {context['task_instance'].task_id}")
    print(f"dag: {base_url}?dag_id={context['dag'].dag_id}{(context['task_instance'])}")


for config_dag in config_dags[:1]:
        
    @dag(
        dag_id=f"k8s_consumer_{config_dag['dag_id']}",
        tags=["harbor"],
        default_args={
            "retries": 0,
            "retry_delay": timedelta(hours=1),
            "owner": config_dag["owner"],
            "on_failure_callback": failed_task_notification,
            "on_retry_callback": task_retry_alert,
               },
        catchup=False,
        schedule_interval="5 0 * * *",
        dagrun_timeout=timedelta(minutes=23 * 60 + 30),
        start_date=days_ago(1),
        max_active_runs=1,
    )
    def k8s_consumer_generator():

        s3_config = Variable.get(key=config_dag["s3__var_id"], deserialize_json=True)
        kafka_config = Variable.get(key=config_dag["kafka__var_id"], deserialize_json=True)
        click_house_config = Variable.get(key=config_dag["click_house__var_id"], deserialize_json=True)
        s3_file_format = config_dag["s3_file_format"]

        @task.kubernetes(
            image="sgnezdilov/kafka_processing:latest",
            namespace=k8s_configs.namespace,
            tolerations=k8s_configs.tolerations,
            affinity=k8s_configs.affinity,
            container_resources=k8s_configs.pod_params[config_dag["resources"]],
        )
        def data_transferring_kafka_to_s3(
            task_id: str, topic: str, kafka_cfg: dict, s3_cfg: dict, s3_file_format: str, click_house_cfg: dict
        ) -> None:
            import sys

            sys.path.append("/usr/src/app/modules")

            from kafka import KafkaConsumer
            from logger import ClickHouseLogger
            from processor import KafkaToS3Processor
            from s3 import S3Writer

            kafka_consumer = KafkaConsumer(kafka_config=kafka_cfg, topic=topic, timeout=60)
            s3_writer = S3Writer(s3_config=s3_cfg, bucket_name="kafka", prefix=task_id, file_format=s3_file_format)
            click_house_logger = ClickHouseLogger(click_house_config=click_house_cfg, source=task_id)
            data_processor = KafkaToS3Processor(kafka_consumer, s3_writer, click_house_logger)

            data_processor.process()

        data_transferring_kafka_to_s3(
            task_id=config_dag["dag_id"],
            topic=config_dag["kafka__topic"],
            kafka_cfg=kafka_config,
            s3_cfg=s3_config,
            s3_file_format=s3_file_format,
            click_house_cfg=click_house_config,
        )

    k8s_consumer_generator()
