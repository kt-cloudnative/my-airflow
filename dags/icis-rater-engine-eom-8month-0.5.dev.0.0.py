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
                , WORKFLOW_NAME='engine-eom-8month',WORKFLOW_ID='7186dfb9ffae4259be2f0246e6a0d77e', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eom-8month-0.5.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 28, 10, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7186dfb9ffae4259be2f0246e6a0d77e')

    eomMainJob_vol = []
    eomMainJob_volMnt = []
    eomMainJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eomMainJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eomMainJob_env = [getICISConfigMap('icis-rater-engine-eom-batch-configmap'), getICISConfigMap('icis-rater-engine-eom-batch-configmap2'), getICISSecret('icis-rater-engine-eom-batch-secret')]
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eomMainJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1ef42d78db304b54b9bde92396cd98fe',
        'volumes': eomMainJob_vol,
        'volume_mounts': eomMainJob_volMnt,
        'env_from':eomMainJob_env,
        'task_id':'eomMainJob',
        'image':'/icis/icis-rater-engine-eom-batch:0.5.0.2',
        'arguments':["--job.names=eomMainJob", "cyclYy=2024", "cyclMonth=08", "run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7186dfb9ffae4259be2f0246e6a0d77e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        eomMainJob,
        Complete
    ]) 

    # authCheck >> eomMainJob >> Complete
    workflow








