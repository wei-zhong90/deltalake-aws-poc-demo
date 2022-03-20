from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import time
from airflow.models import Variable


client = boto3.client('glue')
client1 = boto3.client('s3')

##########################################################
# CONSTANTS AND GLOBAL VARIABLES DEFINITION
##########################################################

# Connection Vars
PROJECT_PARAM = Variable.get("glue_config", deserialize_json=True)

#Get value in list
NumberOfWorkers = PROJECT_PARAM["NumberOfWorkers"]
WorkerType = PROJECT_PARAM["WorkerType"]

### glue job specific variables
# glue_job_name1 = "my_glue_job1"
# glue_job_name2 = "my_glue_job2"
glue_iam_role = "AWSGlueServiceRole"
region_name = "ap-northeast-1"
email_recipient = "me@gmail.com"
##'retry_delay': timedelta(minutes=5)
script_parameter = ["s3://aws-airflow-demo-bucket/GlueScript/01cleaned.py", "s3://aws-airflow-demo-bucket/GlueScript/02joineddimcustomer.py", "s3://aws-airflow-demo-bucket/GlueScript/03goldview.py"]
job_parameter = ["job-stage2", "job-create-processed-base-table"]

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 3, 20),   
    'retries': 2
}

##Athena query
ATHENA_SQL = ['msck repair table processed;']
    
##########################################################
# DYNAMIC FUCNTION FOR GLUE 2.0
##########################################################
def glue2function(**kwargs):
    jobname= kwargs["jobname"]
    scriptlocation = kwargs["scriptlocation"]
    numberffworkers = kwargs["numberofworker"]
    #auto = datetime.now()
    #jobname1=jobname + str(auto)[:-7].strip()

    job = client.start_job_run(JobName=jobname)

    while True:
        status = client.get_job_run(JobName=jobname, RunId=job['JobRunId'])
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':
            break
        time.sleep(10)


##########################################################
# DAG DEFINITIONS
##########################################################
dag = DAG(dag_id= 'delta_optimisation_pipeline_v1',
          schedule_interval= "@hourly",
          catchup=False,
          default_args = default_args
          )


##########################################################
# DUMMY OPERATORS
##########################################################
# task order: first
start_flow = DummyOperator(
    task_id='start_flow',
    trigger_rule="dummy",
    dag=dag)

# task order: last
end_flow = DummyOperator(
    task_id='end_flow',
    dag=dag)



##########################################################
# GLUE 3.0 PYTHON OPERATORS
##########################################################
step1_streaming_trigger_hourly = PythonOperator(
    task_id='step1_streaming_trigger_hourly',
    python_callable=glue2function,
    op_kwargs={'jobname': job_parameter[0], 'scriptlocation': script_parameter[0], 'numberofworker': NumberOfWorkers[2]},
    dag=dag,
    )


step2_generate_symlink = PythonOperator(
    task_id='step_generate_symlink',
    python_callable=glue2function,
    op_kwargs={'jobname': job_parameter[1], 'scriptlocation': script_parameter[1], 'numberofworker': NumberOfWorkers[3]},
    dag=dag,
    )

# glue_step3_goldview_etl = PythonOperator(
#     #task_id='glue_job_step3',
#     task_id='glue_step3_goldview_etl',
#     python_callable=glue2function,
#     op_kwargs={'jobname': job_parameter[2], 'scriptlocation': script_parameter[2], 'numberofworker': NumberOfWorkers[4]},
#     dag=dag,
#     )


##########################################################
#ATHENA DYNAMIC OPERATORS
##########################################################
def create_dynamic_task(index_number, query):
    athena_update_cleaned = AWSAthenaOperator(
        task_id='Athena_Repair_' + str(index_number),
        query=query,
        aws_conn_id='aws_default',
        database='delta_db',
        output_location='s3://delta-lake-streaming-tools/result/',
        dag=dag
    ) 

    return(athena_update_cleaned)


#########################################################
# ATHENA TASK FUNCTION
#########################################################
# Call athena operator
Tasks=[]
for index, x in enumerate(ATHENA_SQL, start=0):
    #task_dynamic_name = 'gm_step' + str(index+1)

    created_task = create_dynamic_task(index, x)
    Tasks.append(created_task)
    

#########################################################
# REDSHIFT TASK FUNCTION
#########################################################
# Call redshift operator


#########################################################
# WORKFLOW DEFINITION
#########################################################

start_flow >> step1_streaming_trigger_hourly >> step2_generate_symlink >> Tasks[0] >> end_flow

  