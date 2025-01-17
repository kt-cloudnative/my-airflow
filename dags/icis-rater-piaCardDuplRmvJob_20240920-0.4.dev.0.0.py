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
                , WORKFLOW_NAME='piaCardDuplRmvJob_20240920',WORKFLOW_ID='af5ee2b7c51949f69b633e31a79a1f86', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-piaCardDuplRmvJob_20240920-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('af5ee2b7c51949f69b633e31a79a1f86')

    piaCardDuplRmvJob_vol = []
    piaCardDuplRmvJob_volMnt = []
    piaCardDuplRmvJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    piaCardDuplRmvJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    piaCardDuplRmvJob_env = [getICISConfigMap('icis-rater-batch-card114-configmap'), getICISConfigMap('icis-rater-batch-card114-configmap2'), getICISSecret('icis-rater-batch-card114-secret')]
    piaCardDuplRmvJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    piaCardDuplRmvJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    piaCardDuplRmvJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3c4f09ba2d164901a35cc96a64d74629',
        'volumes': piaCardDuplRmvJob_vol,
        'volume_mounts': piaCardDuplRmvJob_volMnt,
        'env_from':piaCardDuplRmvJob_env,
        'task_id':'piaCardDuplRmvJob',
        'image':'/icis/icis-rater-batch-card114:20240920121907',
        'arguments':["--job.names=piaCardDuplRmvJob", "runType=T", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('af5ee2b7c51949f69b633e31a79a1f86')

    workflow = COMMON.getICISPipeline([
        authCheck,
        piaCardDuplRmvJob,
        Complete
    ]) 

    # authCheck >> piaCardDuplRmvJob >> Complete
    workflow








