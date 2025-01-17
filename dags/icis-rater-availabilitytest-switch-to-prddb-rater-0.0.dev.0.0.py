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
                , WORKFLOW_NAME='availabilitytest-switch-to-prddb-rater',WORKFLOW_ID='0b56f93d06f04fd19e4d4cd9609f2286', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-availabilitytest-switch-to-prddb-rater-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0b56f93d06f04fd19e4d4cd9609f2286')

    switchtoprddb_vol = []
    switchtoprddb_volMnt = []
    switchtoprddb_env = [getICISConfigMap('icis-rater-availabilitytest-configmap'), getICISConfigMap('icis-rater-availabilitytest-configmap2'), getICISSecret('icis-rater-availabilitytest-secret')]
    switchtoprddb_env.extend([getICISConfigMap('icis-rater-cmmn-configmap'), getICISSecret('icis-rater-cmmn-secret')])
    switchtoprddb_env.extend([getICISConfigMap('icis-rater-truststore.jks')])

    startdaemon = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'startdaemon',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-rater | while read rollname a; do oc scale rollout $rollname -n t-rater --replicas=1; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    startdaemonlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'startdaemonlt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-rater-lt | while read rollname a; do oc scale rollout $rollname -n t-rater-lt --replicas=1; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtoprddb = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddb',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater | while read conf _; do oc patch configmap $conf -n t-rater --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-rater -o=jsonpath='{.data.DB_URL_PRMR}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtoprddbname = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddbname',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater | while read conf a; do oc patch configmap $conf -n t-rater --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_PRMR\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
    switchtoprddblt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddblt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater-lt | while read conf _; do oc patch configmap $conf -n t-rater-lt --type='json' -p=\"[{\\\"op\\\": \\\"replace\\\", \\\"path\\\": \\\"/data/DB_URL\\\", \\\"value\\\": \\\"$(oc get configmap $conf -n t-rater-lt -o=jsonpath='{.data.DB_URL_PRMR}')\\\"}]\"; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    switchtoprddbnamelt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'switchtoprddbnamelt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get configmap -l devpilot/type=online --no-headers -n t-rater-lt | while read conf a; do oc patch configmap $conf -n t-rater-lt --type='json' -p='[{\"op\": \"replace\", \"path\": \"/data/ACTIVE_DB\", \"value\": \"DB_URL_PRMR\" }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
    restartprddbpod = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'restartprddbpod',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-rater | while read rollname a; do oc patch rollout $rollname -n t-rater --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    restartprddbpodlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f7db082bdb644c23a8041c9a4f387192',
        'volumes': switchtoprddb_vol,
        'volume_mounts': switchtoprddb_volMnt,
        'env_from':switchtoprddb_env,
        'task_id':'restartprddbpodlt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=online --no-headers -n t-rater-lt | while read rollname a; do oc patch rollout $rollname -n t-rater-lt --type='json' -p='[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env/-\", \"value\": {\"name\": \"DB_PATCH\", \"value\": \"new_value\"} }]\'; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('0b56f93d06f04fd19e4d4cd9609f2286')

    workflow = COMMON.getICISPipeline([
        authCheck,
        startdaemon,
        startdaemonlt,
        switchtoprddb,
        switchtoprddbname,
        switchtoprddblt,
        switchtoprddbnamelt,
        restartprddbpod,
        restartprddbpodlt,
        Complete
    ]) 

    # authCheck >> switchtoprddb >> Complete
    workflow








