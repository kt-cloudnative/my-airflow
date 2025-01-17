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
                , WORKFLOW_NAME='icis-rater-batch-intltel-shrtLamtMoniJob',WORKFLOW_ID='49d219a8fe4c44edaeda71cd5e2e169f', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intltel-shrtLamtMoniJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 24, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('49d219a8fe4c44edaeda71cd5e2e169f')

    shrtLamtMoniJob_vol = []
    shrtLamtMoniJob_volMnt = []
    shrtLamtMoniJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    shrtLamtMoniJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    shrtLamtMoniJob_env = [getICISConfigMap('icis-rater-batch-intltel-configmap'), getICISConfigMap('icis-rater-batch-intltel-configmap2'), getICISSecret('icis-rater-batch-intltel-secret')]
    shrtLamtMoniJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    shrtLamtMoniJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    shrtLamtMoniJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '51b5b12df0bb4cb48fde22327a8e561e',
        'volumes': shrtLamtMoniJob_vol,
        'volume_mounts': shrtLamtMoniJob_volMnt,
        'env_from':shrtLamtMoniJob_env,
        'task_id':'shrtLamtMoniJob',
        'image':'/icis/icis-rater-batch-intltel:0.4.0.8',
        'arguments':["job.names=shrtLamtMoniJob" , "runType=T", "cyclYmd=20240324"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('49d219a8fe4c44edaeda71cd5e2e169f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        shrtLamtMoniJob,
        Complete
    ]) 

    # authCheck >> shrtLamtMoniJob >> Complete
    workflow








