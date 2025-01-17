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
                , WORKFLOW_NAME='callManager-dev',WORKFLOW_ID='3c34c4c383b44231901df98472dedb83', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-callManager-dev-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3c34c4c383b44231901df98472dedb83')

    callmanagerJob_vol = []
    callmanagerJob_volMnt = []
    callmanagerJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    callmanagerJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    callmanagerJob_env = [getICISConfigMap('icis-rater-batch-callmanager-configmap'), getICISConfigMap('icis-rater-batch-callmanager-configmap2'), getICISSecret('icis-rater-batch-callmanager-secret')]
    callmanagerJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    callmanagerJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    callmanagerJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '18eecfa6d5834b06baea4ab73a7638b7',
        'volumes': callmanagerJob_vol,
        'volume_mounts': callmanagerJob_volMnt,
        'env_from':callmanagerJob_env,
        'task_id':'callmanagerJob',
        'image':'/icis/icis-rater-batch-callmanager:0.4.0.1',
        'arguments':["--job.names=callmanagerJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3c34c4c383b44231901df98472dedb83')

    workflow = COMMON.getICISPipeline([
        authCheck,
        callmanagerJob,
        Complete
    ]) 

    # authCheck >> callmanagerJob >> Complete
    workflow








