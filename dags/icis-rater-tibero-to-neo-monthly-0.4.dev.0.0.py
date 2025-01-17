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
                , WORKFLOW_NAME='tibero-to-neo-monthly',WORKFLOW_ID='6ea70a54c10e4f76a20d739aeea805e0', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-tibero-to-neo-monthly-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 6, 18, 5, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6ea70a54c10e4f76a20d739aeea805e0')

    tiberoToNeoJob_001_vol = []
    tiberoToNeoJob_001_volMnt = []
    tiberoToNeoJob_001_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    tiberoToNeoJob_001_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    tiberoToNeoJob_001_env = [getICISConfigMap('icis-rater-uq-tibero-to-neo-monthy-configmap'), getICISConfigMap('icis-rater-uq-tibero-to-neo-monthy-configmap2'), getICISSecret('icis-rater-uq-tibero-to-neo-monthy-secret')]
    tiberoToNeoJob_001_env.extend([getICISConfigMap('icis-rater-uq-cmmn-configmap'), getICISSecret('icis-rater-uq-cmmn-secret')])
    tiberoToNeoJob_001_env.extend([getICISConfigMap('icis-rater-uq-truststore.jks')])

    tiberoToNeoJob_001 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '47a2c489b5cd4b5f8072e90868b8a5a1',
        'volumes': tiberoToNeoJob_001_vol,
        'volume_mounts': tiberoToNeoJob_001_volMnt,
        'env_from':tiberoToNeoJob_001_env,
        'task_id':'tiberoToNeoJob_001',
        'image':'/icis/icis-rater-uq-tibero-to-neo-monthy:0.4.0.1',
        'arguments':["--job.names=tiberoToNeoJob" "retvYm=202306"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6ea70a54c10e4f76a20d739aeea805e0')

    authCheck >> tiberoToNeoJob_001 >> Complete
    








