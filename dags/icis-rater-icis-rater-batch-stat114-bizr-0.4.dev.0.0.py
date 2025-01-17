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
                , WORKFLOW_NAME='icis-rater-batch-stat114-bizr',WORKFLOW_ID='becf1792436642d4959f3007ece2d82f', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-stat114-bizr-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 11, 21, 38, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('becf1792436642d4959f3007ece2d82f')

    bizr114StatJob_vol = []
    bizr114StatJob_volMnt = []
    bizr114StatJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    bizr114StatJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    bizr114StatJob_env = [getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret'), getICISConfigMap('icis-rater-batch-configmap'), getICISSecret('icis-rater-batch-secret')]
    bizr114StatJob_env.extend([getICISConfigMap('icis-rater-batch-stat114-mng-configmap'), getICISSecret('icis-rater-batch-stat114-mng-secret'), getICISConfigMap('icis-rater-batch-stat114-configmap'), getICISSecret('icis-rater-batch-stat114-secret')])
    bizr114StatJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    bizr114StatJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '92f159c3a1ed4dce8b71a6a48b561ace',
        'volumes': bizr114StatJob_vol,
        'volume_mounts': bizr114StatJob_volMnt,
        'env_from':bizr114StatJob_env,
        'task_id':'bizr114StatJob',
        'image':'/icis/icis-rater-batch-stat114:20241209191303',
        'arguments':["--job.names=bizr114StatJob", "cyclYm=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('becf1792436642d4959f3007ece2d82f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        bizr114StatJob,
        Complete
    ]) 

    # authCheck >> bizr114StatJob >> Complete
    workflow








