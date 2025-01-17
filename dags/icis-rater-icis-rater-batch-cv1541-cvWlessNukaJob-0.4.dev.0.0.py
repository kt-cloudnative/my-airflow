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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-cvWlessNukaJob',WORKFLOW_ID='c1c6da06215641ba9390d7fe7d3959a2', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-cvWlessNukaJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 28, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c1c6da06215641ba9390d7fe7d3959a2')

    cvWlessNukaJob_vol = []
    cvWlessNukaJob_volMnt = []
    cvWlessNukaJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvWlessNukaJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvWlessNukaJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvWlessNukaJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvWlessNukaJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvWlessNukaJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3ad1c752176644a68efabd3bba3756ac',
        'volumes': cvWlessNukaJob_vol,
        'volume_mounts': cvWlessNukaJob_volMnt,
        'env_from':cvWlessNukaJob_env,
        'task_id':'cvWlessNukaJob',
        'image':'/icis/icis-rater-batch-cv1541:0.4.0.38',
        'arguments':["--job.names=cvWlessNukaJob" , "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c1c6da06215641ba9390d7fe7d3959a2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvWlessNukaJob,
        Complete
    ]) 

    # authCheck >> cvWlessNukaJob >> Complete
    workflow








