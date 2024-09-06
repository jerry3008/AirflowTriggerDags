import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'retries' : 1,
    'retry_delay': datetime.timedelta(minutes = 2)
}
#context manager defining the Dags in the Global namespace 
with models.DAG(
    'python_and_bash_with_all_Success_trigger',
    schedule_interval=datetime.timedelta(days=1),
    defalt_args=default_dag_args) as dag:

    def hello_world():
        raise ValueError('Oops! something went wrong. ')
        print('Hello world!')
        return 1
    
    def greeting():
        print('greetings from SpikeysSales! happy shopping.')
        return 'Greeting successfully printed.'
    
    hello_world_greeting = python_operator.PythonOperator(
        task_id='python1',
        python_callable=hello_world)

    spikeysales_greeting = python_operator.PythonOperator(
        task_id='python2',
        python_callable=greeting)
    
    
    bash_greeting = bash_operator.BashOperator(
        task_id='bye_bash',
        bash_command='echo Goodbye! Hope to see you soon.',
        trigger_rule=trigger_rule.TriggerRule.ONE_FAILED)
    
    hello_world_greeting >> spikeysales_greeting >> bash_greeting