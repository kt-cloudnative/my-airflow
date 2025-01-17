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
                , WORKFLOW_NAME='icis-rater-batch-mcid-1',WORKFLOW_ID='4d2937f5902b4b31b3ddca031c350c52', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-mcid-1-0.4.dev.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 14, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4d2937f5902b4b31b3ddca031c350c52')

    famtProdRatJob_vol = []
    famtProdRatJob_volMnt = []
    famtProdRatJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    famtProdRatJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    famtProdRatJob_env = [getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret'), getICISConfigMap('icis-rater-batch-configmap'), getICISSecret('icis-rater-batch-secret')]
    famtProdRatJob_env.extend([getICISConfigMap('icis-rater-batch-mcid-mng-configmap'), getICISSecret('icis-rater-batch-mcid-mng-secret'), getICISConfigMap('icis-rater-batch-mcid-configmap'), getICISSecret('icis-rater-batch-mcid-secret')])
    famtProdRatJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    famtProdRatJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3f79dae4e48b4a41bd56b3f9eef54dbb',
        'volumes': famtProdRatJob_vol,
        'volume_mounts': famtProdRatJob_volMnt,
        'env_from':famtProdRatJob_env,
        'task_id':'famtProdRatJob',
        'image':'/icis/icis-rater-batch-mcid:20250114144240',
        'arguments':["--job.names=famtProdRatJob","cyclYm=202410"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4d2937f5902b4b31b3ddca031c350c52')

    workflow = COMMON.getICISPipeline([
        authCheck,
        famtProdRatJob,
        Complete
    ]) 

    # authCheck >> famtProdRatJob >> Complete
    workflow








