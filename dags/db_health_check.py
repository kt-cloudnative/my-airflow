from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy.exc import OperationalError

# DB 연결 정보 (Airflow Connections 사용 권장)
DB_TYPE = "postgresql"  # 또는 "mysql+pymysql"
DB_HOST = "shclub.synology.me"
DB_PORT = "35444"  # MySQL은 3306
DB_USER = "edu"
DB_PASSWORD = "New1234!"
DB_NAME = "edu"

# SQLAlchemy 연결 URL 생성
DB_URI = f"{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def check_db_health():
    """ 데이터베이스 Health Check 수행 """
    try:
        engine = sqlalchemy.create_engine(DB_URI)
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            if result.scalar() == 1:
                print("✅ DB is healthy")
            else:
                raise Exception("⚠️ Unexpected DB response")
    except OperationalError as e:
        print(f"❌ DB Connection Failed: {e}")
        raise

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="db_health_check",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # 30분마다 실행
    catchup=False,
    tags=["monitoring", "healthcheck"],
) as dag:

    db_health_check_task = PythonOperator(
        task_id="check_db_health",
        python_callable=check_db_health,
        dag=dag,
    )

    db_health_check_task
