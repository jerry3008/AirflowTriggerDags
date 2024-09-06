import random
import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import dummy_operator 

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday
}

with models.DAG(
    'branching_python_operator', 
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args) as dag:

    def greeting():
        print('greetings from spickeysales! Happy shooting.')
        return 'Greeting successfully printed.'
    
    def makeBranchChoice():
        x = random.randint(1, 5)

        if(x <= 2 ):
            return 'hello_spickey'
        
        else:
            return 'dummy'
        
    run_this_first = dummy_operator.DummyOperator(
        task_id='run this first'
    )
    #takes in the python callable this python callable returns the task_id of the next task 
    branching = python_operator.BranchPythonOperator(
        task_id='branching',
        python_callable=makeBranchChoice
    )
    #defining the first part of the pipeline
    run_this_first >> branching

    spikeysales_greeting = python_operator.PythonOperator(
        task_id='Hello Spickey',
        python_callable=greeting)
    
    dummy_followed_python = dummy_operator.DummyOperator(
        task_id='follow_python')
    
    dummy = dummy_operator.DummyOperator(
        task_id= 'dummy')
    
    bash_greeting = bash_operator.BashOperator(
        task_id='bye_bash',
        bash_command='echo goodbye! Hope to see you soon.',
        trigger_rule='one_success'#at least one of its parents needs to succeed
    )
    #two possible parts only one of the two part to run
    branching >> spikeysales_greeting >> dummy_followed_python >> bash_greeting
    branching>> dummy >> bash_greeting
















