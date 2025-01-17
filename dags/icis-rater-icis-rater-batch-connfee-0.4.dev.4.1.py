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
                , WORKFLOW_NAME='icis-rater-batch-connfee',WORKFLOW_ID='b58a3be72f85494aa4494b51a7aeaa22', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-connfee-0.4.dev.4.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 9, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b58a3be72f85494aa4494b51a7aeaa22')

    connfeeSetlJob_vol = []
    connfeeSetlJob_volMnt = []
    connfeeSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    connfeeSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    connfeeSetlJob_env = [getICISConfigMap('icis-rater-batch-connfee-configmap'), getICISConfigMap('icis-rater-batch-connfee-configmap2'), getICISSecret('icis-rater-batch-connfee-secret')]
    connfeeSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    connfeeSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    connfeeSetlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'eb2b6a9190074ed6a8e3e7a1f4bac942',
        'volumes': connfeeSetlJob_vol,
        'volume_mounts': connfeeSetlJob_volMnt,
        'env_from':connfeeSetlJob_env,
        'task_id':'connfeeSetlJob',
        'image':'/icis/icis-rater-batch-connfee:20240812154827',
        'arguments':["--job.names=connfeeSetlJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    connfeeDelJob_vol = []
    connfeeDelJob_volMnt = []
    connfeeDelJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    connfeeDelJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    connfeeDelJob_env = [getICISConfigMap('icis-rater-batch-connfee-configmap'), getICISConfigMap('icis-rater-batch-connfee-configmap2'), getICISSecret('icis-rater-batch-connfee-secret')]
    connfeeDelJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    connfeeDelJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    connfeeDelJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0e7bdeaac32747fab89193283111c4bf',
        'volumes': connfeeDelJob_vol,
        'volume_mounts': connfeeDelJob_volMnt,
        'env_from':connfeeDelJob_env,
        'task_id':'connfeeDelJob',
        'image':'/icis/icis-rater-batch-connfee:20240812154827',
        'arguments':["--job.names=connfeeDelJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b58a3be72f85494aa4494b51a7aeaa22')

    workflow = COMMON.getICISPipeline([
        authCheck,
        connfeeDelJob,
        connfeeSetlJob,
        Complete
    ]) 

    # authCheck >> connfeeDelJob >> connfeeSetlJob >> Complete
    workflow








