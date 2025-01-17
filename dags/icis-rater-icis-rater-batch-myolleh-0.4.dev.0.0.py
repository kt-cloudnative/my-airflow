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
                , WORKFLOW_NAME='icis-rater-batch-myolleh',WORKFLOW_ID='e0a87d59cafb4a9d86a54c9dba4d1ab9', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-myolleh-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e0a87d59cafb4a9d86a54c9dba4d1ab9')

    tel060InfoJob_vol = []
    tel060InfoJob_volMnt = []
    tel060InfoJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    tel060InfoJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    tel060InfoJob_env = [getICISConfigMap('icis-rater-batch-myolleh-configmap'), getICISConfigMap('icis-rater-batch-myolleh-configmap2'), getICISSecret('icis-rater-batch-myolleh-secret')]
    tel060InfoJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    tel060InfoJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    tel060InfoJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cc799eeb0cf84ae9b703a5cbcc23f606',
        'volumes': tel060InfoJob_vol,
        'volume_mounts': tel060InfoJob_volMnt,
        'env_from':tel060InfoJob_env,
        'task_id':'tel060InfoJob',
        'image':'/icis/icis-rater-batch-myolleh:0.4.0.1',
        'arguments':["--job.names=tel060InfoJob" , "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e0a87d59cafb4a9d86a54c9dba4d1ab9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tel060InfoJob,
        Complete
    ]) 

    # authCheck >> tel060InfoJob >> Complete
    workflow








