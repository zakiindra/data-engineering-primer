from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


args = {
    'start_date': datetime(2019,11,5)
}

dag = DAG(
    dag_id='transaction_workflow',
    schedule_interval='0 23 * * *',
    default_args=args
)

get_transaction_from_db = BashOperator(
    task_id='get_transaction_from_db',
    bash_command='python3 ~/iykra/airflow/dags/read_transaction.py',
    dag=dag
)

get_customer_from_api = BashOperator(
    task_id='get_customer_from_api',
    bash_command='python3 ~/iykra/airflow/dags/read_customer.py',
    dag=dag
)

join_customer_transaction = BashOperator(
    task_id='join_customer_transaction',
    bash_command='python3 ~/iykra/airflow/dags/join_customer_transaction.py',
    dag=dag
)

filter_blacklist = BashOperator(
    task_id='filter_blacklist',
    bash_command='python3 ~/iykra/airflow/dags/filter_blacklist.py',
    dag=dag
)

calculate_java_success_transactions = BashOperator(
    task_id='calculate_java_success_transactions',
    bash_command='python3 ~/iykra/airflow/dags/aggregate.py',
    dag=dag
)


join_customer_transaction.set_upstream(get_transaction_from_db)
join_customer_transaction.set_upstream(get_customer_from_api)
filter_blacklist.set_upstream(join_customer_transaction)
calculate_java_success_transactions.set_upstream(filter_blacklist)
