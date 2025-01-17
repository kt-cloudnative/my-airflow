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
                , WORKFLOW_NAME='ocpJob-Dev',WORKFLOW_ID='4ea9855f7e454fd69548c6fcc6a51f73', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-ocpJob-Dev-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4ea9855f7e454fd69548c6fcc6a51f73')

    ocpJob_vol = []
    ocpJob_volMnt = []
    ocpJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ocpJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    ocpJob_env = [getICISConfigMap('icis-rater-batch-ocp-configmap'), getICISConfigMap('icis-rater-batch-ocp-configmap2'), getICISSecret('icis-rater-batch-ocp-secret')]
    ocpJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ocpJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ocpJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '612cd5030551498192e386164f0a1fd5',
        'volumes': ocpJob_vol,
        'volume_mounts': ocpJob_volMnt,
        'env_from':ocpJob_env,
        'task_id':'ocpJob',
        'image':'/icis/icis-rater-batch-ocp:0.4.0.1',
        'arguments':["--job.names=ocpJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4ea9855f7e454fd69548c6fcc6a51f73')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ocpJob,
        Complete
    ]) 

    # authCheck >> ocpJob >> Complete
    workflow








