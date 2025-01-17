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
                , WORKFLOW_NAME='icis-rater-batch-cvdsbl',WORKFLOW_ID='af101eb5830c4c83adf30e987962d6cb', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvdsbl-0.1.dev.3.4'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 7, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('af101eb5830c4c83adf30e987962d6cb')

    lastItgFileCretJob_vol = []
    lastItgFileCretJob_volMnt = []
    lastItgFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lastItgFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lastItgFileCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lastItgFileCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '96b22374653443e0b51eacd91c3374ae',
        'volumes': lastItgFileCretJob_vol,
        'volume_mounts': lastItgFileCretJob_volMnt,
        'env_from':lastItgFileCretJob_env,
        'task_id':'lastItgFileCretJob',
        'image':'/icis/icis-rater-batch-cvdsbl:20240328152045',
        'arguments':["--job.names=lastItgFileCretJob" ,"runType=T" ,"cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    otcomCvNpfaCretJob_vol = []
    otcomCvNpfaCretJob_volMnt = []
    otcomCvNpfaCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    otcomCvNpfaCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    otcomCvNpfaCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcomCvNpfaCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '77243b842eb74efd8b6328cc19194663',
        'volumes': otcomCvNpfaCretJob_vol,
        'volume_mounts': otcomCvNpfaCretJob_volMnt,
        'env_from':otcomCvNpfaCretJob_env,
        'task_id':'otcomCvNpfaCretJob',
        'image':'/icis/icis-rater-batch-cvdsbl:20240328152045',
        'arguments':["--job.names=otcomCvNpfaCretJob" ,"cyclYm=202210" ,"runType=T"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('af101eb5830c4c83adf30e987962d6cb')

    authCheck >> otcomCvNpfaCretJob >> lastItgFileCretJob >> Complete
    








