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
                , WORKFLOW_NAME='icis-rater-batch-hcn',WORKFLOW_ID='706240101fec47f7a28eb9b2e067c123', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-hcn-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 23, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('706240101fec47f7a28eb9b2e067c123')

    bizplcTlkSetlJob_vol = []
    bizplcTlkSetlJob_volMnt = []
    bizplcTlkSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    bizplcTlkSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    bizplcTlkSetlJob_env = [getICISConfigMap('icis-rater-batch-hcn-configmap'), getICISConfigMap('icis-rater-batch-hcn-configmap2'), getICISSecret('icis-rater-batch-hcn-secret')]
    bizplcTlkSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    bizplcTlkSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    bizplcTlkSetlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c0270ccf7a334524b4c90279d467ed5a',
        'volumes': bizplcTlkSetlJob_vol,
        'volume_mounts': bizplcTlkSetlJob_volMnt,
        'env_from':bizplcTlkSetlJob_env,
        'task_id':'bizplcTlkSetlJob',
        'image':'/icis/icis-rater-batch-hcn:0.4.0.1',
        'arguments':["--job.names=bizplcTlkSetlJob" ,"cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('706240101fec47f7a28eb9b2e067c123')

    workflow = COMMON.getICISPipeline([
        authCheck,
        bizplcTlkSetlJob,
        Complete
    ]) 

    # authCheck >> bizplcTlkSetlJob >> Complete
    workflow








