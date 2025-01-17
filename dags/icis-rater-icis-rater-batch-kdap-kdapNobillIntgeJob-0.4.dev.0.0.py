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
                , WORKFLOW_NAME='icis-rater-batch-kdap-kdapNobillIntgeJob',WORKFLOW_ID='c9fee3b45e0249fa90f5e75c5c95c563', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-kdap-kdapNobillIntgeJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c9fee3b45e0249fa90f5e75c5c95c563')

    kdapNobillIntgeJob_vol = []
    kdapNobillIntgeJob_volMnt = []
    kdapNobillIntgeJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kdapNobillIntgeJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kdapNobillIntgeJob_env = [getICISConfigMap('icis-rater-batch-kdap-configmap'), getICISConfigMap('icis-rater-batch-kdap-configmap2'), getICISSecret('icis-rater-batch-kdap-secret')]
    kdapNobillIntgeJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kdapNobillIntgeJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kdapNobillIntgeJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '54756122cf864de4a8ebeaeed3d10fa4',
        'volumes': kdapNobillIntgeJob_vol,
        'volume_mounts': kdapNobillIntgeJob_volMnt,
        'env_from':kdapNobillIntgeJob_env,
        'task_id':'kdapNobillIntgeJob',
        'image':'/icis/icis-rater-batch-kdap:0.4.0.37',
        'arguments':["--job.names=kdapNobillIntgeJob", "retvType=M", "cyclYm=202407", "cyclGroup=1" ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c9fee3b45e0249fa90f5e75c5c95c563')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kdapNobillIntgeJob,
        Complete
    ]) 

    # authCheck >> kdapNobillIntgeJob >> Complete
    workflow








