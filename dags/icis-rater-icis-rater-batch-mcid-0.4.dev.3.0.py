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
                , WORKFLOW_NAME='icis-rater-batch-mcid',WORKFLOW_ID='9d91d8a47b9246859180075ba28527a1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-mcid-0.4.dev.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 21, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9d91d8a47b9246859180075ba28527a1')

    famtProdRatJob_vol = []
    famtProdRatJob_volMnt = []
    famtProdRatJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    famtProdRatJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    famtProdRatJob_env = [getICISConfigMap('icis-rater-batch-mcid-configmap'), getICISConfigMap('icis-rater-batch-mcid-configmap2'), getICISSecret('icis-rater-batch-mcid-secret')]
    famtProdRatJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    famtProdRatJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    famtProdRatJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9c904e90e6f14588b02508a69abaa854',
        'volumes': famtProdRatJob_vol,
        'volume_mounts': famtProdRatJob_volMnt,
        'env_from':famtProdRatJob_env,
        'task_id':'famtProdRatJob',
        'image':'/icis/icis-rater-batch-mcid:20241024093756',
        'arguments':["--job.names=famtProdRatJob","cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9d91d8a47b9246859180075ba28527a1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        famtProdRatJob,
        Complete
    ]) 

    # authCheck >> famtProdRatJob >> Complete
    workflow








