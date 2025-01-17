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
                , WORKFLOW_NAME='icis-rater-batch-cv060-voipInfoChageJob',WORKFLOW_ID='e56e4a4116ca4dceb8ac287e43938e9a', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv060-voipInfoChageJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 19, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e56e4a4116ca4dceb8ac287e43938e9a')

    voipInfoChageJob_vol = []
    voipInfoChageJob_volMnt = []
    voipInfoChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    voipInfoChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data//'))

    voipInfoChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    voipInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    voipInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    voipInfoChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'daa4d9e3a08346d3b22fc85b113683d6',
        'volumes': voipInfoChageJob_vol,
        'volume_mounts': voipInfoChageJob_volMnt,
        'env_from':voipInfoChageJob_env,
        'task_id':'voipInfoChageJob',
        'image':'/icis/icis-rater-batch-cv060:0.4.0.5',
        'arguments':["--job.names=voipInfoChageJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e56e4a4116ca4dceb8ac287e43938e9a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        voipInfoChageJob,
        Complete
    ]) 

    # authCheck >> voipInfoChageJob >> Complete
    workflow








