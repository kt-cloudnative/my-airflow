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
                , WORKFLOW_NAME='CVDSBL_DEV',WORKFLOW_ID='9c4cecf4fe294965945f44309c8edae9', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-CVDSBL_DEV-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 25, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9c4cecf4fe294965945f44309c8edae9')

    lastItgFileCretJob_vol = []
    lastItgFileCretJob_volMnt = []
    lastItgFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lastItgFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lastItgFileCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lastItgFileCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '24b1321c468b4e0fb50c6d10d6e79de0',
        'volumes': lastItgFileCretJob_vol,
        'volume_mounts': lastItgFileCretJob_volMnt,
        'env_from':lastItgFileCretJob_env,
        'task_id':'lastItgFileCretJob',
        'image':'/icis/icis-rater-batch-cvdsbl:20240725155432',
        'arguments':["--job.names=otcomCvNpfaCretJob" "runType=T" "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9c4cecf4fe294965945f44309c8edae9')

    authCheck >> lastItgFileCretJob >> Complete
    








