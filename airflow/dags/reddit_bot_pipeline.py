from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'reddit-bot',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_bot_dataset',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
)

tasks = [
    PythonOperator(task_id='crawl_arctic', python_callable=crawl_arctic_shift, dag=dag),
    PythonOperator(task_id='crawl_flags', python_callable=crawl_community_flags, dag=dag),
    PythonOperator(task_id='parse_silver', python_callable=parse_bronze_to_silver, dag=dag),
    PythonOperator(task_id='compute_features', python_callable=compute_features, dag=dag),
    PythonOperator(task_id='build_graph', python_callable=build_graph, dag=dag),
    PythonOperator(task_id='apply_lfs', python_callable=apply_labeling_functions, dag=dag),
    PythonOperator(task_id='train_gnn', python_callable=train_botsim_rgcn, dag=dag),
    PythonOperator(task_id='train_xgb', python_callable=train_xgboost, dag=dag),
    PythonOperator(task_id='stacking', python_callable=stacking_ensemble, dag=dag),
    PythonOperator(task_id='evaluate', python_callable=evaluate, dag=dag),
    PythonOperator(task_id='export', python_callable=export_dataset, dag=dag),
]

# Dependencies
tasks[0] >> tasks[2] >> tasks[3] >> tasks[4] >> tasks[5]
tasks[1] >> tasks[5]
tasks[5] >> tasks[6] >> tasks[8]
tasks[5] >> tasks[7] >> tasks[8]
tasks[8] >> tasks[9] >> tasks[10]