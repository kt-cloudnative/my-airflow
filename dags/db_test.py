from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
#    KubernetesPodOperator,
#)

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
#import sys
#sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

import time
import socket
import subprocess
import psycopg2
import logging
import random
import threading
import concurrent.futures

#from icis_common import *
#COMMON = ICISCmmn(DOMAIN='sa',ENV='sit', NAMESPACE='t-sa'
#                , WORKFLOW_NAME='sytest',WORKFLOW_ID='61085f55fc364662944f210b7e9d7333', APP_NAME='NBSS_TSA', CHNL_TYPE='TO', USER_ID='91337909')

# 데이터베이스 연결 정보를 전역 변수로 정의
DB_CONFIG = {
    'host': "my-postgresql.airflow.svc.cluster.local",
    'port': "5432",
    'database': "edu",
    'user': "edu",
    'password': "New1234!",
    'connect_timeout': 600
}


with DAG(
    dag_id='db_test',
    schedule_interval='*/2 * * * *',
    start_date= datetime(2025, 3, 10, 0, 0, 00, tzinfo=local_tz),
    end_date= None
    #dagrun_timeout=timedelta(minutes=60),
    #tags=['example', 'example3'],
    #params={"example_key": "example_value"},
) as dag:
    

#with COMMON.getICISDAG({
#    'dag_id':'icis-sa-sytest-sit'
#    ,'schedule_interval':'*/2 * * * *'
#    ,'start_date': datetime(2024, 5, 28, 0, 0, 00, tzinfo=local_tz)
#    ,'end_date': None
#    ,'paused': False
#    ,'max_active_runs':1
#})as dag:

    # authCheck = COMMON.getICISAuthCheckWflow('61085f55fc364662944f210b7e9d7333')

    def test_db_connection(**context):
        logger = logging.getLogger(__name__)
        results = []
        
        logger.info(f"DB 연결 테스트 시작 - {DB_CONFIG['host']}:{DB_CONFIG['port']} (100회 연속 테스트)")
        
        # 연결 성공/실패 통계
        success_count = 0
        failure_count = 0
        min_time = float('inf')
        max_time = 0
        total_time = 0
        
        # 100회 반복 DB 연결 테스트
        for i in range(100):
            start_time = time.time()
            try:
                conn = psycopg2.connect(
                    dbname=DB_CONFIG['database'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    connect_timeout=DB_CONFIG['connect_timeout']
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                conn.close()
                elapsed = time.time() - start_time
                
                success_count += 1
                total_time += elapsed
                min_time = min(min_time, elapsed)
                max_time = max(max_time, elapsed)
                
                # 현재 시간 가져오기
                current_time = pendulum.now(tz=local_tz).format("YYYY-MM-DD HH:mm:ss")
                
                if i % 10 == 0 or elapsed > 1.0:  # 10번마다 로그 출력 또는 1초 이상 걸린 경우
                    message = f"{current_time} - DB 연결 테스트 {i + 1}: 성공 (소요시간: {elapsed:.4f}초)"
                    logger.info(message)
                results.append(f"{current_time} - DB 연결 테스트 {i + 1}: 성공 (소요시간: {elapsed:.4f}초)")
            except Exception as e:
                elapsed = time.time() - start_time
                failure_count += 1
                # 현재 시간 가져오기
                current_time = pendulum.now(tz=local_tz).format("YYYY-MM-DD HH:mm:ss")
                message = f"{current_time} - DB 연결 테스트 {i + 1}: 실패 (소요시간: {elapsed:.4f}초) - 오류: {str(e)}"
                logger.error(message)
                results.append(message)
                raise
            
            # 간격을 0.1~0.3초 사이로 랜덤하게 변경하여 더 실제 환경과 유사하게
            time.sleep(random.uniform(0.1, 0.3))
        
        # 통계 요약
        if success_count > 0:
            avg_time = total_time / success_count
            logger.info(f"DB 연결 테스트 통계: 성공 {success_count}회, 실패 {failure_count}회")
            logger.info(f"평균 연결 시간: {avg_time:.4f}초, 최소: {min_time:.4f}초, 최대: {max_time:.4f}초")
            results.append(f"DB 연결 통계: 성공 {success_count}회, 실패 {failure_count}회")
            results.append(f"평균 연결 시간: {avg_time:.4f}초, 최소: {min_time:.4f}초, 최대: {max_time:.4f}초")
        
        return "\n".join(results)
    
    def test_network_connection(**context):
        logger = logging.getLogger(__name__)
        results = []
        
        logger.info(f"네트워크 연결 테스트 시작 - {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        # 소켓 연결 테스트
        for i in range(100):  # 100회로 증가
            start_time = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)  # 5초 타임아웃 설정
            
            try:
                result = s.connect_ex((DB_CONFIG['host'], int(DB_CONFIG['port'])))
                elapsed = time.time() - start_time
                
                if result == 0:
                    message = f"소켓 연결 테스트 {i+1}: 성공 (소요시간: {elapsed:.4f}초)"
                    if i % 5 == 0:  # 5회마다 로깅
                        logger.info(message)
                else:
                    message = f"소켓 연결 테스트 {i+1}: 실패 (오류코드: {result}, 소요시간: {elapsed:.4f}초)"
                    logger.warning(message)
                
                results.append(message)
            except Exception as e:
                elapsed = time.time() - start_time
                message = f"소켓 연결 테스트 {i+1}: 오류 (소요시간: {elapsed:.4f}초) - {str(e)}"
                logger.error(message)
                results.append(message)
            finally:
                s.close()
            
            time.sleep(random.uniform(0.1, 0.3))
        
        return "\n".join(results)
    
    # 데이터베이스 연결 함수 (재사용을 위한 헬퍼 함수)
    def get_db_connection():
        return psycopg2.connect(
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            connect_timeout=DB_CONFIG['connect_timeout']
        )
    
    # DB 성능 테스트를 위한 태스크
    def test_db_performance(**context):
        logger = logging.getLogger(__name__)
        results = []
        
        try:
            # 연결
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 연결 수 확인
            start_time = time.time()
            cursor.execute("SELECT count(*) FROM pg_stat_activity")
            active_connections = cursor.fetchone()[0]
            elapsed = time.time() - start_time
            
            message = f"활성 연결 수: {active_connections} (쿼리 시간: {elapsed:.4f}초)"
            logger.info(message)
            results.append(message)
            
            # 연결 상태 분포 확인
            start_time = time.time()
            cursor.execute("""
                SELECT state, count(*) 
                FROM pg_stat_activity 
                GROUP BY state
            """)
            connection_states = cursor.fetchall()
            elapsed = time.time() - start_time
            
            message = f"연결 상태 분포 (쿼리 시간: {elapsed:.4f}초):"
            logger.info(message)
            results.append(message)
            
            # 상태별 목록을 위한 상세 정보 딕셔너리
            connection_details = {}
            
            for state, count in connection_states:
                state_str = state if state else "null"
                message = f"  - {state_str}: {count}개"
                logger.info(message)
                results.append(message)

                # 각 상태에 대한 상세 정보 조회
                cursor.execute("""
                    SELECT pid, query 
                    FROM pg_stat_activity 
                    WHERE state = %s
                """, (state,))
                state_details = cursor.fetchall()

                # 상세 정보 목록 추가
                connection_details[state_str] = state_details
                
                if state_details:
                    for pid, query in state_details:
                        message = f"    - PID: {pid}, 쿼리: {query[:300]}..."
                        logger.info(message)
                        results.append(message)
                else:
                    message = f"    - 해당 상태에 대한 연결 정보가 없습니다."
                    logger.info(message)
                    results.append(message)
            
            # 대기 중인 연결 확인
            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM pg_stat_activity 
                WHERE state = 'active' 
                AND wait_event_type IS NOT NULL
            """)
            waiting_connections = cursor.fetchone()[0]
            elapsed = time.time() - start_time
            
            message = f"리소스 대기 중인 연결 수: {waiting_connections} (쿼리 시간: {elapsed:.4f}초)"
            logger.info(message)
            results.append(message)

            # 가장 긴 트랜잭션 확인
            start_time = time.time()
            cursor.execute("""
                SELECT 
                    pid, 
                    now() - xact_start as duration, 
                    state, 
                    query 
                FROM pg_stat_activity 
                WHERE xact_start IS NOT NULL 
                ORDER BY duration DESC 
                LIMIT 3
            """)
            long_transactions = cursor.fetchall()
            elapsed = time.time() - start_time

            message = f"가장 긴 트랜잭션 상위 3개 (쿼리 시간: {elapsed:.4f}초):"
            logger.info(message)
            results.append(message)

            for pid, duration, state, query in long_transactions:
                message = f"  - PID: {pid}, 지속시간: {duration}, 상태: {state}, 쿼리: {query[:300]}..."
                logger.info(message)
                results.append(message)

            # 테이블 크기 확인 
            start_time = time.time()
            cursor.execute("""
                SELECT 
                    table_name, 
                    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as total_size
                FROM 
                    information_schema.tables
                WHERE 
                    table_schema = 'public'
                ORDER BY 
                    pg_total_relation_size(quote_ident(table_name)) DESC
                LIMIT 10
            """)
            table_sizes = cursor.fetchall()
            elapsed = time.time() - start_time
            
            message = f"상위 10개 테이블 크기 (쿼리 시간: {elapsed:.4f}초):"
            logger.info(message)
            results.append(message)
            
            for table in table_sizes:
                message = f"  - {table[0]}: {table[1]}"
                logger.info(message)
                results.append(message)

            conn.close()
            
        except Exception as e:
            message = f"DB 성능 테스트 오류: {str(e)}"
            logger.error(message)
            results.append(message)
        
        return "\n".join(results)
    
    # Airflow 메타데이터 테이블 및 세션 검사
    def check_airflow_metadata(**context):
        logger = logging.getLogger(__name__)
        results = []
        
        try:
            # 연결
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 1. Airflow 주요 테이블 크기 및 레코드 수
            try:
                start_time = time.time()
                cursor.execute("""
                    SELECT 
                        table_name, 
                        pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size,
                        (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
                    FROM 
                        information_schema.tables t
                    WHERE 
                        table_schema = 'public'
                        AND table_name IN ('dag', 'dag_run', 'task_instance', 'log', 'job', 'serialized_dag', 'sla_miss')
                    ORDER BY 
                        pg_total_relation_size(quote_ident(table_name)) DESC
                """)
                airflow_tables = cursor.fetchall()
                elapsed = time.time() - start_time
                
                message = f"Airflow 주요 테이블 정보 (쿼리 시간: {elapsed:.4f}초):"
                logger.info(message)
                results.append(message)
                
                if not airflow_tables:
                    message = "  - Airflow 테이블이 존재하지 않거나 접근 권한이 없습니다."
                    logger.warning(message)
                    results.append(message)
                    # 추가 디버깅: 어떤 테이블이 문제인지 확인
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                    """)
                    existing_tables = [row[0] for row in cursor.fetchall()]
                    debug_msg = f"    - 현재 public 스키마의 테이블: {', '.join(existing_tables) if existing_tables else '없음'}"
                    logger.debug(debug_msg)
                    results.append(debug_msg)
                else:
                    for table in airflow_tables:
                        try:
                            table_name, size, column_count = table
                            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                            record_count = cursor.fetchone()[0]
                            message = f"  - {table_name}: 크기 {size}, 컬럼 수 {column_count}, 레코드 수 {record_count:,}"
                            logger.info(message)
                            results.append(message)
                        except Exception as e:
                            message = f"  - {table_name}: 정보 조회 실패: {str(e)}"
                            logger.error(message)
                            results.append(message)
            except Exception as e:
                message = f"Airflow 주요 테이블 조회 오류: {str(e)}"
                logger.error(message)
                results.append(message)

            # 2. Airflow 관련 활성 세션 조회
            try:
                start_time = time.time()
                cursor.execute("""
                    SELECT 
                        application_name,
                        state,
                        COUNT(*) as session_count,
                        MAX(EXTRACT(EPOCH FROM (NOW() - state_change))) as max_state_duration_sec
                    FROM 
                        pg_stat_activity
                    WHERE 
                        datname = %s
                        AND application_name LIKE 'airflow%'
                    GROUP BY 
                        application_name, state
                    ORDER BY 
                        application_name, state
                """, (DB_CONFIG['database'],))
                airflow_sessions = cursor.fetchall()
                elapsed = time.time() - start_time
                
                message = f"Airflow 활성 세션 현황 (쿼리 시간: {elapsed:.4f}초):"
                logger.info(message)
                results.append(message)
                
                if not airflow_sessions:
                    message = "  - 활성 Airflow 세션이 없습니다."
                    logger.info(message)
                    results.append(message)
                else:
                    for session in airflow_sessions:
                        try:
                            # 안전한 인덱스 접근
                            app_name = session[0]
                            state = session[1]
                            count = session[2]
                            duration = session[3]
                            state_str = state if state else "null"
                            duration_str = f"{duration:.1f}초 전" if duration else "N/A"
                            message = f"  - {app_name} ({state_str}): {count}개 세션, 최장 지속시간: {duration_str}"
                            logger.info(message)
                            results.append(message)
                        except IndexError as e:
                            error_msg = f"    - 세션 데이터 처리 오류: tuple index out of range (데이터: {session})"
                            logger.error(error_msg)
                            results.append(error_msg)
            except Exception as e:
                message = f"Airflow 활성 세션 조회 오류: {str(e)}"
                logger.error(message)
                results.append(message)

            # 3. Heartbeat 관련 정보
            try:
                start_time = time.time()
                cursor.execute("""
                    WITH heartbeat_stats AS (
                        SELECT 
                            job_type,
                            COUNT(*) as job_count,
                            MAX(EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Seoul' - latest_heartbeat AT TIME ZONE 'Asia/Seoul'))) as max_heartbeat_age_sec,
                            MIN(EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Seoul' - latest_heartbeat AT TIME ZONE 'Asia/Seoul'))) as min_heartbeat_age_sec,
                            AVG(EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Seoul' - latest_heartbeat AT TIME ZONE 'Asia/Seoul'))) as avg_heartbeat_age_sec,
                            SUM(CASE WHEN EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Seoul' - latest_heartbeat AT TIME ZONE 'Asia/Seoul')) > 86400 THEN 1 ELSE 0 END) as outdated_count
                        FROM 
                            job 
                        WHERE 
                            latest_heartbeat IS NOT NULL
                        GROUP BY 
                            job_type
                    )
                    SELECT * FROM heartbeat_stats
                    ORDER BY max_heartbeat_age_sec DESC
                """)
                job_heartbeats = cursor.fetchall()
                elapsed = time.time() - start_time
                
                message = f"Airflow Job Heartbeat 정보 (KST 기준, 쿼리 시간: {elapsed:.4f}초):"
                logger.info(message)
                results.append(message)
                
                if not job_heartbeats:
                    message = "  - Heartbeat 정보가 없습니다."
                    logger.info(message)
                    results.append(message)
                else:
                    # 임계값 설정 (1일 = 86400초)
                    threshold = 86400  # 1일 이상은 비정상으로 간주
                    
                    for heartbeat in job_heartbeats:
                        job_type, count, max_age, min_age, avg_age, outdated_count = heartbeat
                        
                        # 기본 메시지
                        message = f"  - {job_type}: {count}개 작업, 최근 heartbeat: 최소 {min_age:.1f}초 전, 평균 {avg_age:.1f}초 전, 최대 {max_age:.1f}초 전, 1일 이상 지난 작업: {outdated_count}개"
                        logger.info(message)
                        results.append(message)
                        
                        # 이상치 감지 및 경고
                        if max_age > threshold or avg_age > threshold:
                            warning_msg = f"    ! 경고: {job_type}에서 비정상적인 heartbeat 감지 (최대: {max_age/86400:.1f}일, 평균: {avg_age/86400:.1f}일)"
                            logger.warning(warning_msg)
                            results.append(warning_msg)
                            
                            # 오래된 heartbeat 상세 정보 조회 (KST 기준)
                            cursor.execute("""
                                SELECT 
                                    j.id AS job_id,
                                    j.latest_heartbeat,
                                    EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Asia/Seoul' - j.latest_heartbeat AT TIME ZONE 'Asia/Seoul')) AS age_seconds,
                                    ti.dag_id,
                                    ti.task_id
                                FROM 
                                    job j
                                LEFT JOIN 
                                    task_instance ti ON j.id = ti.job_id
                                WHERE 
                                    j.job_type = %s
                                    AND j.latest_heartbeat < NOW() AT TIME ZONE 'Asia/Seoul' - INTERVAL '1 day'
                                ORDER BY 
                                    j.latest_heartbeat ASC
                                LIMIT 5
                            """, (job_type,))
                            old_heartbeats = cursor.fetchall()
                            
                            if old_heartbeats:
                                detail_msg = f"    - 오래된 heartbeat 샘플 (최대 5개, KST 기준):"
                                logger.debug(detail_msg)
                                results.append(detail_msg)
                                for job_id, latest_hb, age_se, dag_id, task_id in old_heartbeats:
                                    detail_msg = (
                                        f"      - Job ID: {job_id}, "
                                        f"latest heartbeat: {latest_hb}, "
                                        f"초: {age_se / 86400:.1f}일, "
                                        f"DAG ID: {dag_id}, "
                                        f"작업 ID: {task_id} "
                                    )
                                    logger.debug(detail_msg)
                                    results.append(detail_msg)
                            else:
                                detail_msg = f"    - 오래된 heartbeat 상세 정보 없음"
                                logger.debug(detail_msg)
                                results.append(detail_msg)

            except Exception as e:
                message = f"Heartbeat 정보 조회 오류: {str(e)}"
                logger.error(message)
                results.append(message)

            # 4. 오래된 작업과 실행 중인 작업
            start_time = time.time()
            cursor.execute("""
                SELECT 
                    state, 
                    COUNT(*) as count
                FROM 
                    task_instance
                GROUP BY 
                    state
                ORDER BY 
                    count DESC
            """)
            task_states = cursor.fetchall()
            elapsed = time.time() - start_time
            
            message = f"태스크 인스턴스 상태 분포 (쿼리 시간: {elapsed:.4f}초):"
            logger.info(message)
            results.append(message)
            
            if not task_states:
                logger.error("태스크 인스턴스 상태 정보가 없습니다.")
            else:
                for state, count in task_states:
                    message = f"  - {state}: {count:,}개"
                    logger.info(message)
                    results.append(message)

            # 5. 오래된 레코드 수
            try:
                start_time = time.time()
                cursor.execute("""
                    SELECT 
                        'dag_run' as table_name, 
                        COUNT(*) as old_records 
                    FROM 
                        dag_run 
                    WHERE 
                        end_date < NOW() - INTERVAL '30 days'
                    UNION ALL
                    SELECT 
                        'task_instance' as table_name, 
                        COUNT(*) as old_records 
                    FROM 
                        task_instance 
                    WHERE 
                        end_date < NOW() - INTERVAL '30 days'
                    UNION ALL
                    SELECT 
                        'log' as table_name, 
                        COUNT(*) as old_records 
                    FROM 
                        log 
                    WHERE 
                        dttm < NOW() - INTERVAL '30 days'
                """)
                old_records = cursor.fetchall()
                elapsed = time.time() - start_time
                
                message = f"30일 이상 지난 레코드 수 (쿼리 시간: {elapsed:.4f}초):"
                logger.info(message)
                results.append(message)
                
                if not old_records:
                    message = "  - 오래된 레코드가 없습니다."
                    logger.info(message)
                    results.append(message)
                else:
                    for record in old_records:
                        table, count = record
                        message = f"  - {table}: {count:,}개"
                        logger.info(message)
                        results.append(message)
            except Exception as e:
                message = f"오래된 레코드 조회 오류: {str(e)}"
                logger.error(message)
                results.append(message)

            # 6. 장기 실행 중인 세션 확인
            try:
                start_time = time.time()
                cursor.execute("""
                    SELECT 
                        pid, 
                        application_name,
                        client_addr,
                        state,
                        NOW() - state_change as state_duration,
                        NOW() - query_start as query_duration,
                        query
                    FROM 
                        pg_stat_activity 
                    WHERE 
                        datname = %s
                        AND state = 'active'
                        AND query_start < NOW() - INTERVAL '5 minutes'
                        AND query NOT LIKE '%%pg_stat_activity%%'
                    ORDER BY 
                        query_duration DESC
                    LIMIT 5
                """, (DB_CONFIG['database'],))
                long_sessions = cursor.fetchall()
                elapsed = time.time() - start_time
                
                message = f"5분 이상 실행 중인 쿼리 (쿼리 시간: {elapsed:.4f}초):"
                logger.info(message)
                results.append(message)
                
                if not long_sessions:
                    message = "  - 5분 이상 실행 중인 쿼리가 없습니다."
                    logger.info(message)
                    results.append(message)
                else:
                    for session in long_sessions:
                        pid, app, addr, state, state_dur, query_dur, query = session
                        message = f"  - PID {pid} ({app}): {query_dur} 동안 실행 중, 상태: {state}, 쿼리: {query[:100]}..."
                        logger.info(message)
                        results.append(message)
            except Exception as e:
                message = f"장기 실행 세션 조회 오류: {str(e)}"
                logger.error(message)
                results.append(message)
                
            conn.close()
            
        except Exception as e:
            message = f"Airflow 메타데이터 검사 오류: {str(e)}"
            logger.error(message)
            results.append(message)
        
        return "\n".join(results)

    
    # DB 연결 테스트 태스크
    db_test = PythonOperator(
        task_id='db_connection_test',
        python_callable=test_db_connection,
        dag=dag,
        do_xcom_push=False
    )
    
    # 네트워크 연결 테스트 태스크
    #network_test = PythonOperator(
    #    task_id='network_connection_test',
    #    python_callable=test_network_connection,
    #    dag=dag,
    #    do_xcom_push=False
    #)
    
    # DB 성능 테스트 태스크
    #db_performance_test = PythonOperator(
    #    task_id='db_performance_test',
    #    python_callable=test_db_performance,
    #    dag=dag,
    #    do_xcom_push=False
    #)
    
    # Airflow 메타데이터 검사 태스크
    #metadata_test = PythonOperator(
    #    task_id='airflow_metadata_test',
    #    python_callable=check_airflow_metadata,
    #    dag=dag,
    #    do_xcom_push=False
    #)

    # Complete = COMMON.getICISCompleteWflowTask('61085f55fc364662944f210b7e9d7333')

    # authCheck >> db_test >> network_test >> db_performance_test >> metadata_test >> Complete

    # db_test >> network_test >> db_performance_test >> metadata_test
    
    ##db_performance_test.trigger_rule = 'none_failed_min_one_success'

    ##[db_test, network_test] >> db_performance_test >> metadata_test

    db_test >> Complete
