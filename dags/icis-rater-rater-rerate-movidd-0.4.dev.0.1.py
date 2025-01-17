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
                , WORKFLOW_NAME='rater-rerate-movidd',WORKFLOW_ID='c140963b61b148dc94d21a4bc0c1ac24', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-rater-rerate-movidd-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 25, 0, 4, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c140963b61b148dc94d21a4bc0c1ac24')

    shreFreeChgJob_vol = []
    shreFreeChgJob_volMnt = []
    shreFreeChgJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    shreFreeChgJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    shreFreeChgJob_env = [getICISConfigMap('icis-rater-batch-rerate-configmap'), getICISConfigMap('icis-rater-batch-rerate-configmap2'), getICISSecret('icis-rater-batch-rerate-secret')]
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    shreFreeChgJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9f5db0b1d2a747c99380d3576247dbf1',
        'volumes': shreFreeChgJob_vol,
        'volume_mounts': shreFreeChgJob_volMnt,
        'env_from':shreFreeChgJob_env,
        'task_id':'shreFreeChgJob',
        'image':'/icis/icis-rater-batch-rerate:20240823181858',
        'arguments':["--job.names=shreFreeChgJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c140963b61b148dc94d21a4bc0c1ac24')

    workflow = COMMON.getICISPipeline([
        authCheck,
        shreFreeChgJob,
        Complete
    ]) 

    # authCheck >> shreFreeChgJob >> Complete
    workflow








