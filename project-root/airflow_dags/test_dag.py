from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG("test_dag", schedule_interval=None, default_args=default_args, catchup=False) as dag:
    stock_list = ["apple", "samsung", "skhynix", "nvidia"]

    for stock in stock_list:
        
        crawl = BashOperator(
            task_id=f"crawl_{stock}",
            bash_command=(
                f"STOCK_NAME={stock} "
                f"PYTHONPATH=/root/project-root "
                f"python /root/project-root/scripts/crawling.py"
            )
        )
        
        preprocess = BashOperator(
            task_id=f"preprocess_{stock}",
            bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/preprocess_main.py"
        )

        label = BashOperator(
            task_id=f"label_{stock}",
            bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/labeling_main.py"
        )
        
        inference = BashOperator(
        task_id=f"infer_{stock}",
        bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/inference.py"
        )
        
        aggregate = BashOperator(
        task_id=f"aggregate_{stock}",
        bash_command=f"STOCK_NAME={stock} PYTHONPATH=/root/project-root python /root/project-root/scripts/aggregate.py"
        )

        crawl >> preprocess >> label >> inference >> aggregate
