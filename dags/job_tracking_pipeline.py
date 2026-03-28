from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Config ────────────────────────────────────────────────────────
MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_DATABASE = "logs"
MYSQL_USER     = "root"
MYSQL_PASSWORD = "1"

PROJECT_PATH   = "/mnt/c/Users/Admin/OneDrive/Desktop/Realtime_project"


# ── Task 1: Kiểm tra MySQL còn kết nối được không ─────────────────
def check_mysql():
    import mysql.connector
    cnx = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        database=MYSQL_DATABASE, user=MYSQL_USER, password=MYSQL_PASSWORD,
    )
    cursor = cnx.cursor()
    cursor.execute("SELECT COUNT(*) FROM job")
    count = cursor.fetchone()[0]
    cnx.close()
    print(f"[check_mysql] OK — {count} jobs in MySQL")
    return count


# ── Task 2: Gửi 1 batch events vào Kafka ──────────────────────────
def run_producer_batch():
    import subprocess
    result = subprocess.run(
        ["python3", f"{PROJECT_PATH}/scripts/kafka_producer_batch.py"],
        capture_output=True, text=True, timeout=60,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Producer failed:\n{result.stderr}")
    print("[run_producer_batch] OK — batch sent to Kafka")


# ── Task 3: Chờ Spark Structured Streaming xử lý xong ─────────────
def wait_for_spark():
    print("[wait_for_spark] Waiting 30s for Spark micro-batch to process...")
    time.sleep(30)
    print("[wait_for_spark] Done")


# ── Task 4: Verify data đã được ghi vào MySQL ─────────────────────
def verify_data():
    import mysql.connector
    cnx = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        database=MYSQL_DATABASE, user=MYSQL_USER, password=MYSQL_PASSWORD,
    )
    cursor = cnx.cursor()

    cursor.execute("SELECT COUNT(*) FROM events")
    total_rows = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(clicks), SUM(conversion), ROUND(SUM(spend_hour),0) FROM events")
    row = cursor.fetchone()
    total_clicks = row[0] or 0
    total_conv   = row[1] or 0
    total_spend  = row[2] or 0
    cnx.close()

    print(f"[verify_data] events table: {total_rows} rows")
    print(f"[verify_data] clicks={total_clicks} | conversions={total_conv} | spend={total_spend}")

    if total_rows == 0:
        raise Exception("[verify_data] FAIL — events table is empty!")
    print("[verify_data] PASS ✓")


# ── DAG definition ─────────────────────────────────────────────────
default_args = {
    "owner"          : "airflow",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=2),
}

with DAG(
    dag_id                 = "job_tracking_pipeline",
    default_args           = default_args,
    description            = "check MySQL → send Kafka batch → wait Spark → verify MySQL",
    schedule_interval      = "* * * * *",      # chạy mỗi 1 phút
    start_date             = datetime(2026, 3, 28),
    catchup                = False,
    is_paused_upon_creation= False,
    tags                   = ["kafka", "spark", "mysql", "batch", "orchestration"],
) as dag:
    t1 = PythonOperator(task_id="check_mysql",        python_callable=check_mysql)
    t2 = PythonOperator(task_id="run_producer_batch", python_callable=run_producer_batch)
    t3 = PythonOperator(task_id="wait_for_spark",     python_callable=wait_for_spark)
    t4 = PythonOperator(task_id="verify_data",        python_callable=verify_data)
    t1 >> t2 >> t3 >> t4
