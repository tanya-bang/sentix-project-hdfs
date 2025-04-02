from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG("preprocess_and_label_dag", schedule_interval=None, default_args=default_args, catchup=False) as dag:
    stock_list = ["apple", "samsung", "skhynix", "nvidia"]

    for stock in stock_list:

        preprocess = BashOperator(
            task_id=f"preprocess_{stock}",
            bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/preprocess_main.py"
        )

        label = BashOperator(
            task_id=f"label_{stock}",
            bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/labeling_main.py"
        )

        preprocess >> label
