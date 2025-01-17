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
                , WORKFLOW_NAME='icis-rater-batch-konet',WORKFLOW_ID='30830d6506be498888ff084bd4833c2c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-konet-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 2, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('30830d6506be498888ff084bd4833c2c')

    konetSlngJob_vol = []
    konetSlngJob_volMnt = []
    konetSlngJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    konetSlngJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    konetSlngJob_env = [getICISConfigMap('icis-rater-batch-konet-configmap'), getICISConfigMap('icis-rater-batch-konet-configmap2'), getICISSecret('icis-rater-batch-konet-secret')]
    konetSlngJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    konetSlngJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    konetSlngJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1c91e10d8a2c4600acb6cce922e7a3d9',
        'volumes': konetSlngJob_vol,
        'volume_mounts': konetSlngJob_volMnt,
        'env_from':konetSlngJob_env,
        'task_id':'konetSlngJob',
        'image':'/icis/icis-rater-batch-konet:20240802142524',
        'arguments':["--job.names=konetSlngJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('30830d6506be498888ff084bd4833c2c')

    authCheck >> konetSlngJob >> Complete
    








