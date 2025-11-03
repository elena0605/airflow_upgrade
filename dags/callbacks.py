import os
from airflow.exceptions import AirflowFailException


def check_logs_for_errors(context):
    task_instance = context['task_instance']

    # Retrieve the necessary components for the log file path
    dag_id = task_instance.dag_id  # DAG ID from the task instance
    run_id = task_instance.run_id  # Current run ID
    task_id = task_instance.task_id  # Task ID from the task instance
    try_number = task_instance.try_number

    # Construct log file path
    log_base_path = os.getenv("AIRFLOW__CORE__BASE_LOG_FOLDER", "/opt/airflow/logs")
    print(f"Base log folder: {log_base_path}")
    log_file_path = os.path.join(
        log_base_path,
        f"dag_id={dag_id}",     
        f"run_id={run_id}",    
        f"task_id={task_id}",   
        f"attempt={try_number}.log"
    )
 
    print(f"Constructed log file path: {log_file_path}")

    try:
        # Check if the log file exists
        if not os.path.isfile(log_file_path):
            raise FileNotFoundError(f"Log file not found: {log_file_path}")

        # Read log file contents
        with open(log_file_path, "r") as log_file:
            log_lines = [line.strip() for line in log_file if line.strip()]

        # Check for error lines
        error_lines = [line for line in log_lines if "ERROR" in line and "Exception" in line]
        http_error_lines = [
            line for line in log_lines
            if any(f"HTTP {http_code}" in line for http_code in ["500", "404", "403", "502", "503"])
        ]

        if error_lines:
            print(f"Found error lines: {error_lines}")
            raise AirflowFailException(f"Errors found in log file: {log_file_path}")

        if http_error_lines:
            print(f"Found HTTP error lines: {http_error_lines}")
            raise AirflowFailException(f"HTTP Error Lines: {http_error_lines}")

    except FileNotFoundError as e:
        print(f"Log file not found: {e}")
    except Exception as e:
        print(f"Unexpected error while checking logs: {e}")
        



def task_failure_callback(context):
    """
    Callback triggered on task failure.
    """
    task_instance = context['task_instance']
    # Use logical_date for Airflow 2.x (backward compatible with execution_date)
    execution_date = getattr(task_instance, 'logical_date', None) or getattr(task_instance, 'execution_date', 'N/A')
    print(
        f"Task {task_instance.task_id} in DAG {task_instance.dag_id} failed "
        f"on {execution_date}. See logs for details."
    )
    check_logs_for_errors(context)

def task_success_callback(context):
    """
    Callback triggered on task success.
    """
    task_instance = context['task_instance']
    try:
        print("Checking task logs for errors post-success.")
        check_logs_for_errors(context)
    except AirflowFailException as e:
        # Log the specific failure reason
        print(f"Task failed due to: {e}")
        raise
    except Exception as e:
        # Handle unexpected errors gracefully
        print(f"Unhandled error in callback: {e}")
        raise
