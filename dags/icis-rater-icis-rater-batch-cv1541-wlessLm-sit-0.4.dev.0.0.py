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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-wlessLm-sit',WORKFLOW_ID='085b9d316a104ac583a6d1d39fb6496e', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-wlessLm-sit-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('085b9d316a104ac583a6d1d39fb6496e')

    cvWlessLmJob_vol = []
    cvWlessLmJob_volMnt = []
    cvWlessLmJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvWlessLmJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvWlessLmJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvWlessLmJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvWlessLmJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvWlessLmJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5fa858d6f09844a5bd8ff0a9465928c6',
        'volumes': cvWlessLmJob_vol,
        'volume_mounts': cvWlessLmJob_volMnt,
        'env_from':cvWlessLmJob_env,
        'task_id':'cvWlessLmJob',
        'image':'/icis/icis-rater-batch-cv1541:0.4.0.14',
        'arguments':["--job.names=cvWlessLmJob" , "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('085b9d316a104ac583a6d1d39fb6496e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvWlessLmJob,
        Complete
    ]) 

    # authCheck >> cvWlessLmJob >> Complete
    workflow








