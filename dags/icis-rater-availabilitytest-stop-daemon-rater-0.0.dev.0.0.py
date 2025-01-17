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
                , WORKFLOW_NAME='availabilitytest-stop-daemon-rater',WORKFLOW_ID='ab1375fb6eb14fad8f0824eac162a467', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-availabilitytest-stop-daemon-rater-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 4, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ab1375fb6eb14fad8f0824eac162a467')

    stopdaemon_vol = []
    stopdaemon_volMnt = []
    stopdaemon_env = [getICISConfigMap('icis-rater-availabilitytest-configmap'), getICISConfigMap('icis-rater-availabilitytest-configmap2'), getICISSecret('icis-rater-availabilitytest-secret')]
    stopdaemon_env.extend([getICISConfigMap('icis-rater-cmmn-configmap'), getICISSecret('icis-rater-cmmn-secret')])
    stopdaemon_env.extend([getICISConfigMap('icis-rater-truststore.jks')])

    stopdaemon = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a09e6405a4d942e0b563efd32111e1f4',
        'volumes': stopdaemon_vol,
        'volume_mounts': stopdaemon_volMnt,
        'env_from':stopdaemon_env,
        'task_id':'stopdaemon',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-rater | while read rollname a; do oc scale rollout $rollname -n t-rater --replicas=0; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    stopdaemonlt = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a09e6405a4d942e0b563efd32111e1f4',
        'volumes': stopdaemon_vol,
        'volume_mounts': stopdaemon_volMnt,
        'env_from':stopdaemon_env,
        'task_id':'stopdaemonlt',
        'image':'/icis/icis-rater-availabilitytest:20240925102437',
        'arguments':["oc get rollout -l devpilot/type=daemon --no-headers -n t-rater-lt | while read rollname a; do oc scale rollout $rollname -n t-rater --replicas=0; done || echo 'done' "],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ab1375fb6eb14fad8f0824eac162a467')

    workflow = COMMON.getICISPipeline([
        authCheck,
        stopdaemon,
        stopdaemonlt,
        Complete
    ]) 

    # authCheck >> stopdaemon >> Complete
    workflow








