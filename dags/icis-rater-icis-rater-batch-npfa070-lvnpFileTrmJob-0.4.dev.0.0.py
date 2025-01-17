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
                , WORKFLOW_NAME='icis-rater-batch-npfa070-lvnpFileTrmJob',WORKFLOW_ID='2c43225abd164c2e87da3e57857c71d3', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-npfa070-lvnpFileTrmJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 24, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2c43225abd164c2e87da3e57857c71d3')

    lvnpFileTrmJob_vol = []
    lvnpFileTrmJob_volMnt = []
    lvnpFileTrmJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lvnpFileTrmJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lvnpFileTrmJob_env = [getICISConfigMap('icis-rater-batch-npfa070-configmap'), getICISConfigMap('icis-rater-batch-npfa070-configmap2'), getICISSecret('icis-rater-batch-npfa070-secret')]
    lvnpFileTrmJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lvnpFileTrmJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lvnpFileTrmJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '42eecf999c8544d89be36b38849bf0ed',
        'volumes': lvnpFileTrmJob_vol,
        'volume_mounts': lvnpFileTrmJob_volMnt,
        'env_from':lvnpFileTrmJob_env,
        'task_id':'lvnpFileTrmJob',
        'image':'/icis/icis-rater-batch-npfa070:0.4.0.5',
        'arguments':["--job.names=lvnpFileTrmJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2c43225abd164c2e87da3e57857c71d3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        lvnpFileTrmJob,
        Complete
    ]) 

    # authCheck >> lvnpFileTrmJob >> Complete
    workflow








