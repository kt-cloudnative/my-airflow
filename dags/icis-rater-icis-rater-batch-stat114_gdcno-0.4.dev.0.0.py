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
                , WORKFLOW_NAME='icis-rater-batch-stat114_gdcno',WORKFLOW_ID='c6eddb41667c49b9bf5eef9997812717', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-stat114_gdcno-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 5, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c6eddb41667c49b9bf5eef9997812717')

    gdncNo114StatJob_vol = []
    gdncNo114StatJob_volMnt = []
    gdncNo114StatJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    gdncNo114StatJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    gdncNo114StatJob_env = [getICISConfigMap('icis-rater-batch-stat114-configmap'), getICISConfigMap('icis-rater-batch-stat114-configmap2'), getICISSecret('icis-rater-batch-stat114-secret')]
    gdncNo114StatJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    gdncNo114StatJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    gdncNo114StatJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4feee771e9d04da899b47fc00f62bb24',
        'volumes': gdncNo114StatJob_vol,
        'volume_mounts': gdncNo114StatJob_volMnt,
        'env_from':gdncNo114StatJob_env,
        'task_id':'gdncNo114StatJob',
        'image':'/icis/icis-rater-batch-stat114:20240805133250',
        'arguments':["--job.names=gdncNo114StatJob", "runType=T", "cyclYm=202405"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c6eddb41667c49b9bf5eef9997812717')

    authCheck >> gdncNo114StatJob >> Complete
    








