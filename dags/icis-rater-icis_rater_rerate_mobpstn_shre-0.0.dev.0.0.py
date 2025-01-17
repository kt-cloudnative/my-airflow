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
                , WORKFLOW_NAME='icis_rater_rerate_mobpstn_shre',WORKFLOW_ID='6224bc9912b3443ca35aa8466a66d174', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis_rater_rerate_mobpstn_shre-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6224bc9912b3443ca35aa8466a66d174')

    shreFreeChgJob_vol = []
    shreFreeChgJob_volMnt = []
    shreFreeChgJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    shreFreeChgJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    shreFreeChgJob_env = [getICISConfigMap('icis-rater-batch-rerate-configmap'), getICISConfigMap('icis-rater-batch-rerate-configmap2'), getICISSecret('icis-rater-batch-rerate-secret')]
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    shreFreeChgJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '10932fe2cf064cfe88dc42c741f2acb8',
        'volumes': shreFreeChgJob_vol,
        'volume_mounts': shreFreeChgJob_volMnt,
        'env_from':shreFreeChgJob_env,
        'task_id':'shreFreeChgJob',
        'image':'/icis/icis-rater-batch-rerate:0.4.0.1',
        'arguments':["--job.names=shreFreeChgJob", "runType=T", "cyclYm=202410"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6224bc9912b3443ca35aa8466a66d174')

    workflow = COMMON.getICISPipeline([
        authCheck,
        shreFreeChgJob,
        Complete
    ]) 

    # authCheck >> shreFreeChgJob >> Complete
    workflow








