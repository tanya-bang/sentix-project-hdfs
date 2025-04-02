from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="aggregate_only_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    
    stock_list = ["apple", "samsung", "skhynix", "nvidia"]

    for stock in stock_list:
        aggregate_task = BashOperator(
            task_id=f"aggregate_{stock}",
            bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/aggregate.py"
        )
