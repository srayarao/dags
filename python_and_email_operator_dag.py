from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #    'email_on_failure': False,
    #    'email_on_retry': False,
    #    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def build_email(**context):
    with NamedTemporaryFile(mode='w+', suffix=".txt") as file:
        email_content = "Hello World"
        return email_content


with DAG(
        "email_example",
        description="Sample Email Example with File attachments",
        schedule_interval="@once",
        start_date=datetime(2020, 3, 17),
        default_args=default_args,
        catchup=False
) as dag:
    python_op = PythonOperator(
        task_id='build_email',
        python_callable=build_email,
        provide_context=True,
        dag=dag
    )

    email_op = EmailOperator(
        task_id='send_email',
        to='{{ var.value.get("email_list") }}',
        subject="Test Email Please Ignore",
        html_content='{{task_instance.xcom_pull(task_ids="build_email")}}',
        #files=['{{task_instance.xcom_pull(task_ids="build_email")}}'],
    )

    python_op >> email_op
