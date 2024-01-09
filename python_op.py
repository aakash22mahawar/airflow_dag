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


   def perform_math_operation(ti):   ##xcom_pull example
       num1 = ti.xcom_pull(task_ids='xcom_ops_push',key='num1')
       num2 = ti.xcom_pull(task_ids='xcom_ops_push', key='num2')
       result = num1 * num2
       print(f"The result of the mathematical operation is: {result}")
       return result


   # Task 2: PythonOperator to perform a mathematical operation
   task_math_operation = PythonOperator(
       task_id='perform_math_ops',
       python_callable=perform_math_operation,
       dag=dag
   )


   def xcoms_pull(num3, ti):
       pull_val = ti.xcom_pull(task_ids='perform_math_ops')
       print(num3 * pull_val)
       return num3 * pull_val


   xcom_task_pull = PythonOperator(
       task_id='xcom_ops_pull',
       python_callable=xcoms_pull,
       op_kwargs={'num3': 10},
       dag=dag
   )


   def xcoms_push(ti):
       ti.xcom_push(key='num1',value=25)
       ti.xcom_push(key='num2',value=100)




   xcom_task_push = PythonOperator(
       task_id='xcom_ops_push',
       python_callable=xcoms_push,
       op_kwargs={'num3': 10},
       dag=dag
   )


   # Set task dependencies
   task_print_message >>  xcom_task_push >>  task_math_operation >> xcom_task_pull

