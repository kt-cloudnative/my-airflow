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
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater'
                , WORKFLOW_NAME='availabilitytest-switch-to-dsstdb-rater',WORKFLOW_ID='72657a0e2dbb4877bc7810dc0f9c25d6', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-availabilitytest-switch-to-dsstdb-rater-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('72657a0e2dbb4877bc7810dc0f9c25d6')

    switchtodsstdb_vol = []
    switchtodsstdb_volMnt = []
    switchtodsstdb_env = [getICISConfigMap('icis-rater-availabilitytest-configmap'), getICISConfigMap('icis-rater-availabilitytest-configmap2'), getICISSecret('icis-rater-availabilitytest-secret')]
    switchtodsstdb_env.extend([getICISConfigMap('icis-rater-cmmn-configmap'), getICISSecret('icis-rater-cmmn-secret')])
    switchtodsstdb_env.extend([getICISConfigMap('icis-rater-truststore.jks')])

    switchtodsstdb = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdb',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater | while read conf _; do oc patch configmap $conf -n t-rater --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-rater -o=jsonpath='{.data.DB_URL_SCND}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtodsstdbname = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdbname',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater | while read conf a; do oc patch configmap $conf -n t-rater --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_SCND\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
    switchtodsstdblt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdblt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater-lt | while read conf _; do oc patch configmap $conf -n t-rater-lt --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-rater-lt -o=jsonpath='{.data.DB_URL_SCND}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtodsstdbnamelt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'switchtodsstdbnamelt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater-lt | while read conf a; do oc patch configmap $conf -n t-rater-lt --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_SCND\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
       
    restartdsstdbpod = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'restartdsstdbpod',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-rater | while read rollname a; do oc patch rollout $rollname -n t-rater --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    restartdsstdbpodlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '68019c9e30294d5d993fcc4e09825f10',
        'volumes': switchtodsstdb_vol,
        'volume_mounts': switchtodsstdb_volMnt,
        'env_from':switchtodsstdb_env,
        'task_id':'restartdsstdbpodlt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-rater-lt | while read rollname a; do oc patch rollout $rollname -n t-rater-lt --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('72657a0e2dbb4877bc7810dc0f9c25d6')

    workflow = COMMON.getICISPipeline([
        authCheck,
        switchtodsstdb,
        switchtodsstdbname,
        switchtodsstdblt,
        switchtodsstdbnamelt,
        restartdsstdbpod,
        restartdsstdbpodlt,
        Complete
    ]) 

    # authCheck >> switchtodsstdb >> Complete
    workflow








