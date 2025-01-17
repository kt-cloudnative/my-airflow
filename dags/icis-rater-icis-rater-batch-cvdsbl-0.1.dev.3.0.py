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

from icis_common_staging import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvdsbl-0.1.dev.3.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 7, 12, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('30dd8b64701f4f19ab5d7e610d1b0aeb')

    otcomCvNpfaCretJob_vol = []
    otcomCvNpfaCretJob_volMnt = []
    otcomCvNpfaCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    otcomCvNpfaCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    otcomCvNpfaCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcomCvNpfaCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : '23e6f12fed544a52b0c9acc22db0552b',
        'volumes': otcomCvNpfaCretJob_vol,
        'volume_mounts': otcomCvNpfaCretJob_volMnt,
        'env_from':otcomCvNpfaCretJob_env,
        'task_id':'otcomCvNpfaCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvdsbl:20240328152045',
        'arguments':["--job.names=otcomCvNpfaCretJob cyclYm=202210 runType=T"]
    })


    lastItgFileCretJob_vol = []
    lastItgFileCretJob_volMnt = []
    lastItgFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lastItgFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    lastItgFileCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lastItgFileCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'dde176de6e8d4434934086ef78304847',
        'volumes': lastItgFileCretJob_vol,
        'volume_mounts': lastItgFileCretJob_volMnt,
        'env_from':lastItgFileCretJob_env,
        'task_id':'lastItgFileCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvdsbl:20240328152045',
        'arguments':["--job.names=lastItgFileCretJob runType=T cyclYm=202306"]
    })


    Complete = getICISCompleteWflowTask('30dd8b64701f4f19ab5d7e610d1b0aeb')

    authCheck >> otcomCvNpfaCretJob >> lastItgFileCretJob >> Complete
    








