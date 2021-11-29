from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import datetime

VERSION = '0.1.7' # increment this each version of the DAG

DAG_NAME = 'ga4-transformation-' + VERSION

default_args = {
    'start_date': days_ago(1),  # change this to a fixed date for backfilling
    'email_on_failure': True,
    'email': 'mark@example.com',
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=10),
    'project_id': 'learning-ga4',
    'execution_timeout': datetime.timedelta(minutes=60)
}

schedule_interval = '2 4 * * *'  # min, hour, day of month, month, day of week

dag = DAG(DAG_NAME, default_args=default_args, schedule_interval=schedule_interval)


start = DummyOperator(
    task_id='start',
    dag=dag
)

# uses the Airflow macro {{ ds_nodash }} to insert todays date in YYYYMMDD form
check_table = BigQueryCheckOperator(
    task_id='check_table',
    dag=dag,
    sql='''
    SELECT count(1) > 5000 
    FROM `learning-ga4.analytics_250021309.events_{{ ds_nodash }}`"
    '''
)

checked = DummyOperator(
    task_id='checked',
    dag=dag
)

# a function so you can loop over many tables, SQL files
def make_bq(table_id):

    task = BigQueryOperator(
        task_id='make_bq_'+table_id,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table='learning_ga4.ga4_aggregations.{}${{ ds_nodash}}'.format(table_id),
        sql='./ga4_sql/{}.sql'.format(table_id),
        use_legacy_sql=False,
        dag=dag
    )

    return task

ga_tables = [
  'pageview-aggs',
  'ga4-join-crm',
  'ecom-fields'
]

ga_aggregations = [] # helpful if you are doing other downstream transformations
for table in ga_tables:
  task = make_bq(table)
  checked >> task
  ga_aggregations.append(task)


# create the DAG 
start >> check_table >> checked