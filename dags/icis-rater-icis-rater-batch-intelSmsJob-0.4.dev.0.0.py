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
                , WORKFLOW_NAME='icis-rater-batch-intelSmsJob',WORKFLOW_ID='c8202c9136874a1f89a072b5d3862299', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intelSmsJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c8202c9136874a1f89a072b5d3862299')

    intelSmsJob_vol = []
    intelSmsJob_volMnt = []
    intelSmsJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intelSmsJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intelSmsJob_env = [getICISConfigMap('icis-rater-batch-billif-configmap'), getICISConfigMap('icis-rater-batch-billif-configmap2'), getICISSecret('icis-rater-batch-billif-secret')]
    intelSmsJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intelSmsJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intelSmsJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0b3cbd33e6224165a015b250eea4f713',
        'volumes': intelSmsJob_vol,
        'volume_mounts': intelSmsJob_volMnt,
        'env_from':intelSmsJob_env,
        'task_id':'intelSmsJob',
        'image':'/icis/icis-rater-batch-billif:0.4.0.22',
        'arguments':["--job.names=intelSmsJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c8202c9136874a1f89a072b5d3862299')

    workflow = COMMON.getICISPipeline([
        authCheck,
        intelSmsJob,
        Complete
    ]) 

    # authCheck >> intelSmsJob >> Complete
    workflow








