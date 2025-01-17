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
    'dag_id':'icis-rater-icis-rater-batch-cv114-wless-0.2.dev.4.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('6d423f2b03474b5dbbe8bc4ab837258a')

    cv114WlessJob_vol = []
    cv114WlessJob_volMnt = []
    cv114WlessJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cv114WlessJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cv114WlessJob_env = [getICISConfigMap('icis-rater-batch-cv114-configmap'), getICISConfigMap('icis-rater-batch-cv114-configmap2'), getICISSecret('icis-rater-batch-cv114-secret')]
    cv114WlessJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cv114WlessJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cv114WlessJob = COMMON.getICISKubernetesPodOperator({
        'id' : '5a7801236af74a7b9d79dab3c5195bdd',
        'volumes': cv114WlessJob_vol,
        'volume_mounts': cv114WlessJob_volMnt,
        'env_from':cv114WlessJob_env,
        'task_id':'cv114WlessJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv114:20240311151246',
        'arguments':["--job.names=cv114WlessJob", "runType=T", "cyclYm=202306", "cyclStDt=20230628"]
    })


    cv114WlessSumJob_vol = []
    cv114WlessSumJob_volMnt = []
    cv114WlessSumJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cv114WlessSumJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cv114WlessSumJob_env = [getICISConfigMap('icis-rater-batch-cv114-configmap'), getICISConfigMap('icis-rater-batch-cv114-configmap2'), getICISSecret('icis-rater-batch-cv114-secret')]
    cv114WlessSumJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cv114WlessSumJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cv114WlessSumJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'af1e551ecd9a4a8dae6ecccd10524d55',
        'volumes': cv114WlessSumJob_vol,
        'volume_mounts': cv114WlessSumJob_volMnt,
        'env_from':cv114WlessSumJob_env,
        'task_id':'cv114WlessSumJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv114:20240311151246',
        'arguments':["--job.names=cv114WlessSumJob", "runType=T", "cyclYm=202306", "nowDate=20230625"]
    })


    Complete = getICISCompleteWflowTask('6d423f2b03474b5dbbe8bc4ab837258a')

    authCheck >> cv114WlessJob >> cv114WlessSumJob >> Complete
    








