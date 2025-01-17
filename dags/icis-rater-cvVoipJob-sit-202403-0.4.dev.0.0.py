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
                , WORKFLOW_NAME='cvVoipJob-sit-202403',WORKFLOW_ID='905cbf5bef3d416ba9b50325639f165c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-cvVoipJob-sit-202403-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 19, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('905cbf5bef3d416ba9b50325639f165c')

    cvVoipJob_vol = []
    cvVoipJob_volMnt = []
    cvVoipJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvVoipJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvVoipJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvVoipJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvVoipJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvVoipJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '52ba1af1095447f49c4c9c0543a3be74',
        'volumes': cvVoipJob_vol,
        'volume_mounts': cvVoipJob_volMnt,
        'env_from':cvVoipJob_env,
        'task_id':'cvVoipJob',
        'image':'/icis/icis-rater-batch-cv1541:0.4.0.12',
        'arguments':["--job.names=cvVoipJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('905cbf5bef3d416ba9b50325639f165c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvVoipJob,
        Complete
    ]) 

    # authCheck >> cvVoipJob >> Complete
    workflow








