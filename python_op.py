from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Airflow DAG configuration
default_args = {
    "owner": "Airflow",
    "start_date": datetime.now(),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# Instantiate a DAG with the provided default_args
with DAG('python_dag', default_args=default_args, description='python operator', schedule_interval=None) as dag:
    # Define a Python function to print a message
    def print_message():
        print("Hello from the PythonOperator!")

    # Task 1: PythonOperator to print a message
    task_print_message = PythonOperator(
        task_id='print_message',
        python_callable=print_message,
        dag=dag,
    )

    # Define a Python function to perform a mathematical operation
    def perform_math_operation(num1, num2):
        result = num1 * num2
        print(f"The result of the mathematical operation is: {result}")
        return result

    # Task 2: PythonOperator to perform a mathematical operation
    task_math_operation = PythonOperator(
        task_id='perform_math_ops',
        python_callable=perform_math_operation,
        op_kwargs={'num1': 5, 'num2': 3},
        dag=dag
    )

    def xcoms_pull(num3, ti):
        pull_val = ti.xcom_pull(task_ids='perform_math_ops')
        print(num3 * pull_val)
        return num3 * pull_val

    xcom_task = PythonOperator(
        task_id='xcom_ops',
        python_callable=xcoms_pull,
        op_kwargs={'num3': 10},
        dag=dag
    )

    # Set task dependencies
    task_print_message >> task_math_operation >> xcom_task
