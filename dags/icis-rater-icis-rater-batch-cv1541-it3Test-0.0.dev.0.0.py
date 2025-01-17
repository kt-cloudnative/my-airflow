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

from icis_common_rater_temp import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-it3Test-0.0.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 12, 8, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('967d24ff329c4f3cbbd389d4c6081429')

    cvVoipJob_vol = []
    cvVoipJob_volMnt = []
    cvVoipJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvVoipJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvVoipJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvVoipJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvVoipJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvVoipJob = COMMON.getICISKubernetesPodOperator({
        'id' : '5c2ed6a9a01a47178d5f4e12acd0abb8',
        'volumes': cvVoipJob_vol,
        'volume_mounts': cvVoipJob_volMnt,
        'env_from':cvVoipJob_env,
        'task_id':'cvVoipJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv1541:20231208155718',
        'arguments':["--job.names=cvVoipJob" , "runType=T" ,"useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('967d24ff329c4f3cbbd389d4c6081429')

    authCheck >> cvVoipJob >> Complete
    








