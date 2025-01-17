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
                , WORKFLOW_NAME='icis-rater-batch-connfee_ins',WORKFLOW_ID='c603857af65f43e2b0dfc7c64890e1f8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-connfee_ins-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 13, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c603857af65f43e2b0dfc7c64890e1f8')

    connfeeSetlJob_vol = []
    connfeeSetlJob_volMnt = []
    connfeeSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    connfeeSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    connfeeSetlJob_env = [getICISConfigMap('icis-rater-batch-connfee-configmap'), getICISConfigMap('icis-rater-batch-connfee-configmap2'), getICISSecret('icis-rater-batch-connfee-secret')]
    connfeeSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    connfeeSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    connfeeSetlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '65960a21b736485999227264255c2de9',
        'volumes': connfeeSetlJob_vol,
        'volume_mounts': connfeeSetlJob_volMnt,
        'env_from':connfeeSetlJob_env,
        'task_id':'connfeeSetlJob',
        'image':'/icis/icis-rater-batch-connfee:20240813162708',
        'arguments':["--job.names=connfeeSetlJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c603857af65f43e2b0dfc7c64890e1f8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        connfeeSetlJob,
        Complete
    ]) 

    # authCheck >> connfeeSetlJob >> Complete
    workflow








