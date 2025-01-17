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
                , WORKFLOW_NAME='icis-rater-batch-bjslng',WORKFLOW_ID='6d7b81e7df744e57953424c69b31746f', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-bjslng-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 8, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6d7b81e7df744e57953424c69b31746f')

    bJVoIPSlngDatCretJob_vol = []
    bJVoIPSlngDatCretJob_volMnt = []
    bJVoIPSlngDatCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    bJVoIPSlngDatCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc',' /batch_data/'))

    bJVoIPSlngDatCretJob_env = [getICISConfigMap('icis-rater-batch-bjslng-configmap'), getICISConfigMap('icis-rater-batch-bjslng-configmap2'), getICISSecret('icis-rater-batch-bjslng-secret')]
    bJVoIPSlngDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    bJVoIPSlngDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    bJVoIPSlngDatCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b92bae205aa8457791d7c5576f56ddbd',
        'volumes': bJVoIPSlngDatCretJob_vol,
        'volume_mounts': bJVoIPSlngDatCretJob_volMnt,
        'env_from':bJVoIPSlngDatCretJob_env,
        'task_id':'bJVoIPSlngDatCretJob',
        'image':'/icis/icis-rater-batch-bjslng:0.4.0.13',
        'arguments':["--job.names=bJVoIPSlngDatCretJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6d7b81e7df744e57953424c69b31746f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        bJVoIPSlngDatCretJob,
        Complete
    ]) 

    # authCheck >> bJVoIPSlngDatCretJob >> Complete
    workflow








