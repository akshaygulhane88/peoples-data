from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.trigger_rule import TriggerRule
import json 
import boto3
from datetime import datetime
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
import pendulum
import logging,traceback
import airflow
from airflow import Dataset


# retrieving config properties
sns = Variable.get("sns_mirror")
environment = Variable.get("env")

default_params = {"default_args": {"owner": "CDP T1", "catchup": "False", "retries": "0", "concurrency": 1}}
global_params = {"email": ["cdp_t1_&_ews@mckinsey.com"], "email_on_failure": "True", "email_on_retry": "False", "schedule_interval": "50 */3 * * *", "start_date": "2022-05-18 11:15", "region_name": "us-east-1", "max_active_runs": 1, "concurrency": 20}
all_env_params = {"dev": {"dynamic_vars": {"$peopledb": "ENT_PEOPLE_DEV_DB", "$firmdb": "ENT_FIRM_DEV_DB", "$env": "DEV", "$dq_trigger_bucket": "cdp-data-people-849242744489-us-east-1-dev", "$cdp_trigger_key": "dq/trigger_file/cdp", "$cdb_trigger_key": "dq/trigger_file/cdb", "$dq_trigger_target_load_key": "dq/trigger_file/cdp_target_load", "$acc_id": "849242744489", "$date": "16-Jan-2021", "$region_name": "us-east-1"}}, "qa": {"dynamic_vars": {"$peopledb": "ENT_PEOPLE_QA_DB", "$firmdb": "ENT_FIRM_QA_DB", "$env": "QA", "$dq_trigger_bucket": "cdp-data-people-648624564125-us-east-1-qa", "$cdp_trigger_key": "dq/trigger_file/cdp", "$cdb_trigger_key": "dq/trigger_file/cdb", "$dq_trigger_target_load_key": "dq/trigger_file/cdp_target_load", "$acc_id": "648624564125", "$date": "16-Jan-2021", "$region_name": "us-east-1"}}, "prod": {"dynamic_vars": {"$peopledb": "ENT_PEOPLE_PROD_DB", "$firmdb": "ENT_FIRM_PROD_DB", "$env": "PROD", "$dq_trigger_bucket": "cdp-data-people-596125291709-us-east-1-prod", "$cdp_trigger_key": "dq/trigger_file/cdp", "$cdb_trigger_key": "dq/trigger_file/cdb", "$dq_trigger_target_load_key": "dq/trigger_file/cdp_target_load", "$acc_id": "596125291709", "$date": "16-Jan-2021", "$region_name": "us-east-1"}}}
default_args = {"owner": "CDP T1", "catchup": "False", "retries": "0", "concurrency": 1}
env_params = all_env_params.get(environment)
dynamic_vars = env_params['dynamic_vars']

start_time_str = global_params['start_date']
start_time = pendulum.from_format(start_time_str, 'YYYY-MM-DD HH:mm', tz='US/Eastern')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


#Call back function incase of a Airflow DAG failuer
def on_failure_callback(context):
    status = True
    dag_id = context["task_instance"].dag_id
    dag_run = context.get("dag_run")
    task_instances = dag_run.get_task_instances(state='failed')

    failed_task_list = []
    for failed_task in task_instances:
        dict_failed_task = {}
        dict_failed_task['task_id'] =  str(failed_task.task_id)
        dict_failed_task['state'] =  str(failed_task.state)
        dict_failed_task['duration'] =  str(failed_task.duration)
        dict_failed_task['start_date'] =  str(failed_task.start_date)
        dict_failed_task['end_date'] =  str(failed_task.end_date)
        failed_task_list.append(dict_failed_task)

    op = SnsPublishOperator(
        task_id="failure",
        aws_conn_id="aws_default",
        target_arn=sns,
        message=str(failed_task_list),
        subject="Airflow DAG - {dag_id} Failed".format(dag_id=dag_id),
    )
    status = op.execute(context=context)
    
    return status 
    
def failure_func():
    raise AirflowException()


# [Initialize Dynamic Variables] 
peopledb=dynamic_vars['$peopledb']
firmdb=dynamic_vars['$firmdb']
env=dynamic_vars['$env']
dq_trigger_bucket=dynamic_vars['$dq_trigger_bucket']
cdp_trigger_key=dynamic_vars['$cdp_trigger_key']
cdb_trigger_key=dynamic_vars['$cdb_trigger_key']
dq_trigger_target_load_key=dynamic_vars['$dq_trigger_target_load_key']
acc_id=dynamic_vars['$acc_id']
date=dynamic_vars['$date']
region_name=dynamic_vars['$region_name']

dataset_tobe_lookedup_1=Dataset(f's3://{dq_trigger_bucket}/COMPLETED_CDP_REFERENCE_LKP_MASTER_LOAD.txt')
with DAG(dag_id="CDP_TARGET_PEOPLE_MASTER",on_failure_callback=on_failure_callback,
	 default_args = default_args, start_date = start_time, catchup=False,schedule=[dataset_tobe_lookedup_1], max_active_runs=global_params.get('max_active_runs',1),concurrency=global_params.get('concurrency',25)) as dag:


	# [Adding Dummy Operator for this Task]
	start = DummyOperator(task_id='start')
	# [STARTING a new Task Group]
	with TaskGroup("PER_EXPERTISE_TOPIC", tooltip="Tasks for PER_EXPERTISE_TOPIC") as PER_EXPERTISE_TOPIC:
		# [Adding GlueJobOperator for this Task]
		glue_operator_STAGING_PERSON_EXPERTISE_TOPIC_STG_GLUE = GlueJobOperator(
                                                                                task_id=f"STAGING_PERSON_EXPERTISE_TOPIC_STG_GLUE",
                                                                                job_name=f"STAGING_PERSON_EXPERTISE_TOPIC_STG_GLUE",
                                                                                job_desc=f"triggering glue job STAGING_PERSON_EXPERTISE_TOPIC_STG_GLUE",
                                                                                region_name=f"us-east-1",
                                                                                verbose=True,
                                                                                script_args={'config_bucket': f"cdp-data-glue-{acc_id}-{region_name}-{env}"},
                                                                                trigger_rule=TriggerRule.ALL_SUCCESS)
		# [Adding GlueJobOperator for this Task]
		glue_operator_TRANSFORM_ED_PERSON_EXPERTISE_TOPIC_D_GLUE = GlueJobOperator(
                                                                                task_id=f"TRANSFORM_ED_PERSON_EXPERTISE_TOPIC_D_GLUE",
                                                                                job_name=f"TRANSFORM_ED_PERSON_EXPERTISE_TOPIC_D_GLUE",
                                                                                job_desc=f"triggering glue job TRANSFORM_ED_PERSON_EXPERTISE_TOPIC_D_GLUE",
                                                                                region_name=f"us-east-1",
                                                                                verbose=True,
                                                                                script_args={'config_bucket': f"cdp-data-glue-{acc_id}-{region_name}-{env}"},
                                                                                trigger_rule=TriggerRule.ALL_SUCCESS)

		glue_operator_STAGING_PERSON_EXPERTISE_TOPIC_STG_GLUE >> glue_operator_TRANSFORM_ED_PERSON_EXPERTISE_TOPIC_D_GLUE

	with TaskGroup("GENERATE_DAG_COMPLETION_DATASETS", tooltip="Tasks for GENERATE_DAG_COMPLETION_DATASETS") as GENERATE_DAG_COMPLETION_DATASETS:
		# [Adding BashOperator for this Task]
		dataset_tobe_created_1=Dataset(f's3://{dq_trigger_bucket}/COMPLETED_CDP_TARGET_PEOPLE_MASTER.txt')
		

		bash_DAG_COMPLETION_DATASETS = BashOperator(task_id=f"DAG_COMPLETION_DATASETS",
                                                                    bash_command="sleep 5",
                                                                    outlets=[dataset_tobe_created_1])

		[bash_DAG_COMPLETION_DATASETS]


	# [Adding Dummy Operator for this Task]
	end = DummyOperator(task_id='end')
	start >> [PER_EXPERTISE_TOPIC] >> GENERATE_DAG_COMPLETION_DATASETS >> end