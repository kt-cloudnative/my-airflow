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
                , WORKFLOW_NAME='icis-rater-batch-slng_dev01',WORKFLOW_ID='7caa10331d664e3791a3880d41126db0', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-slng_dev01-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 6, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7caa10331d664e3791a3880d41126db0')

    dayPresmSlngJob_vol = []
    dayPresmSlngJob_volMnt = []
    dayPresmSlngJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    dayPresmSlngJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    dayPresmSlngJob_env = [getICISConfigMap('icis-rater-batch-slng-configmap'), getICISConfigMap('icis-rater-batch-slng-configmap2'), getICISSecret('icis-rater-batch-slng-secret')]
    dayPresmSlngJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    dayPresmSlngJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    dayPresmSlngJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8ead4fbbebc94338bd3e6e56a84d3d59',
        'volumes': dayPresmSlngJob_vol,
        'volume_mounts': dayPresmSlngJob_volMnt,
        'env_from':dayPresmSlngJob_env,
        'task_id':'dayPresmSlngJob',
        'image':'/icis/icis-rater-batch-slng:20240626104641',
        'arguments':["--job.names=dayPresmSlngJob", "runType=T" , "wrkDateVal=20240529"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7caa10331d664e3791a3880d41126db0')

    authCheck >> dayPresmSlngJob >> Complete
    








