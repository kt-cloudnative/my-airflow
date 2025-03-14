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
    'dag_id':'icis-rater-icis-rater-batch-cvdsbl-0.1.dev.0.1',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 7, 12, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('e5a6946008334aae8c197107c6a0e139')

    otcomCvNpfaCretJob_vol = []
    otcomCvNpfaCretJob_volMnt = []
    otcomCvNpfaCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    otcomCvNpfaCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    otcomCvNpfaCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'),getICISSecret('icis-rater-batch-cvdsbl-secret')]
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    otcomCvNpfaCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcomCvNpfaCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : '0a8690f4fb0a410aa2dd021e86caedc9',
        'volumes': otcomCvNpfaCretJob_vol,
        'volume_mounts': otcomCvNpfaCretJob_volMnt,
        'env_from':otcomCvNpfaCretJob_env,
        'task_id':'otcomCvNpfaCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvdsbl:20230707154343',
        'arguments':["--job.names=otcomCvNpfaCretJob cyclMon=10"]
    })


    lastItgFileCretJob_vol = []
    lastItgFileCretJob_volMnt = []
    lastItgFileCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    lastItgFileCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    lastItgFileCretJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lastItgFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lastItgFileCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : '052bea2134364c7d9655ccb12e8cba88',
        'volumes': lastItgFileCretJob_vol,
        'volume_mounts': lastItgFileCretJob_volMnt,
        'env_from':lastItgFileCretJob_env,
        'task_id':'lastItgFileCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvdsbl:20230707154343',
        'arguments':["--job.names=lastItgFileCretJob billMonth=202211"]
    })


    Complete = getICISCompleteWflowTask('e5a6946008334aae8c197107c6a0e139')

    authCheck >> otcomCvNpfaCretJob >> lastItgFileCretJob >> Complete
    








