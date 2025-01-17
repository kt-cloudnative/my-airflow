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
                , WORKFLOW_NAME='icis-rater-batch-cvcc-dev',WORKFLOW_ID='4092bc9b713a4c4d998a634aa378a8da', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvcc-dev-0.4.dev.1.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 5, 13, 9, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4092bc9b713a4c4d998a634aa378a8da')

    intlCcVoipCvJob_vol = []
    intlCcVoipCvJob_volMnt = []
    intlCcVoipCvJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intlCcVoipCvJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intlCcVoipCvJob_env = [getICISConfigMap('icis-rater-batch-cvcc-configmap'), getICISConfigMap('icis-rater-batch-cvcc-configmap2'), getICISSecret('icis-rater-batch-cvcc-secret')]
    intlCcVoipCvJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intlCcVoipCvJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intlCcVoipCvJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6815c2fe78784fb3862dd7552556d048',
        'volumes': intlCcVoipCvJob_vol,
        'volume_mounts': intlCcVoipCvJob_volMnt,
        'env_from':intlCcVoipCvJob_env,
        'task_id':'intlCcVoipCvJob',
        'image':'/icis/icis-rater-batch-cvcc:20240514091624',
        'arguments':["--job.names=intlCcVoipCvJob", "runType=T", "cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4092bc9b713a4c4d998a634aa378a8da')

    authCheck >> intlCcVoipCvJob >> Complete
    








