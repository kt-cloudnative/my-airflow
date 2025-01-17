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
                , WORKFLOW_NAME='icis-rater-batch-npfa070',WORKFLOW_ID='d47f6c9bf1684edd8cc23f62009054eb', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-npfa070-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 24, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d47f6c9bf1684edd8cc23f62009054eb')

    kt070npfaNoCretJob_vol = []
    kt070npfaNoCretJob_volMnt = []
    kt070npfaNoCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kt070npfaNoCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kt070npfaNoCretJob_env = [getICISConfigMap('icis-rater-batch-npfa070-configmap'), getICISConfigMap('icis-rater-batch-npfa070-configmap2'), getICISSecret('icis-rater-batch-npfa070-secret')]
    kt070npfaNoCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kt070npfaNoCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kt070npfaNoCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ec9b848d05854f7ea79d8402b2e3adfc',
        'volumes': kt070npfaNoCretJob_vol,
        'volume_mounts': kt070npfaNoCretJob_volMnt,
        'env_from':kt070npfaNoCretJob_env,
        'task_id':'kt070npfaNoCretJob',
        'image':'/icis/icis-rater-batch-npfa070:0.4.0.1',
        'arguments':["--job.names=kt070npfaNoCretJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    lvnpFileTrmJoba_vol = []
    lvnpFileTrmJoba_volMnt = []
    lvnpFileTrmJoba_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lvnpFileTrmJoba_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lvnpFileTrmJoba_env = [getICISConfigMap('icis-rater-batch-npfa070-configmap'), getICISConfigMap('icis-rater-batch-npfa070-configmap2'), getICISSecret('icis-rater-batch-npfa070-secret')]
    lvnpFileTrmJoba_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lvnpFileTrmJoba_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lvnpFileTrmJoba = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1f3dc08e77a0498da685fa0b56cf6b26',
        'volumes': lvnpFileTrmJoba_vol,
        'volume_mounts': lvnpFileTrmJoba_volMnt,
        'env_from':lvnpFileTrmJoba_env,
        'task_id':'lvnpFileTrmJoba',
        'image':'/icis/icis-rater-batch-npfa070:0.4.0.1',
        'arguments':["--job.names=lvnpFileTrmJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d47f6c9bf1684edd8cc23f62009054eb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kt070npfaNoCretJob,
        lvnpFileTrmJoba,
        Complete
    ]) 

    # authCheck >> kt070npfaNoCretJob >> lvnpFileTrmJoba >> Complete
    workflow








