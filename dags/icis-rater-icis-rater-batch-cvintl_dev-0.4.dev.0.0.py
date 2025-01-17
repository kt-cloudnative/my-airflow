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
                , WORKFLOW_NAME='icis-rater-batch-cvintl_dev',WORKFLOW_ID='3e361604d9b54cba95c1d7d28bc453d5', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvintl_dev-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 22, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3e361604d9b54cba95c1d7d28bc453d5')

    cvIntlOtcVoipJob_vol = []
    cvIntlOtcVoipJob_volMnt = []
    cvIntlOtcVoipJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvIntlOtcVoipJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvIntlOtcVoipJob_env = [getICISConfigMap('icis-rater-batch-cvintl-configmap'), getICISConfigMap('icis-rater-batch-cvintl-configmap2'), getICISSecret('icis-rater-batch-cvintl-secret')]
    cvIntlOtcVoipJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvIntlOtcVoipJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvIntlOtcVoipJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '622ec08355d8443a8ddf4f34fcb87510',
        'volumes': cvIntlOtcVoipJob_vol,
        'volume_mounts': cvIntlOtcVoipJob_volMnt,
        'env_from':cvIntlOtcVoipJob_env,
        'task_id':'cvIntlOtcVoipJob',
        'image':'/icis/icis-rater-batch-cvintl:20240722122631',
        'arguments':["--job.names=cvIntlOtcVoipJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3e361604d9b54cba95c1d7d28bc453d5')

    authCheck >> cvIntlOtcVoipJob >> Complete
    








