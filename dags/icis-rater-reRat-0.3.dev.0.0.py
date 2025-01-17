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
    'dag_id':'icis-rater-reRat-0.3.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2024, 3, 26, 0, 1, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('be31712b90b3463b832f2974cfb6fdcd')

    shreFreeChgJob_vol = []
    shreFreeChgJob_volMnt = []
    shreFreeChgJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    shreFreeChgJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    shreFreeChgJob_env = [getICISConfigMap('icis-rater-batch-rerate-configmap'), getICISConfigMap('icis-rater-batch-rerate-configmap2'), getICISSecret('icis-rater-batch-rerate-secret')]
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    shreFreeChgJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    shreFreeChgJob = COMMON.getICISKubernetesPodOperator({
        'id' : '5c700b4b631d4d69a33a26a1dfb72688',
        'volumes': shreFreeChgJob_vol,
        'volume_mounts': shreFreeChgJob_volMnt,
        'env_from':shreFreeChgJob_env,
        'task_id':'shreFreeChgJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-rerate:20240326151020',
        'arguments':["--job.names=shreFreeChgJob", "runType=T", "cyclYm=202311"]
    })


    Complete = getICISCompleteWflowTask('be31712b90b3463b832f2974cfb6fdcd')

    authCheck >> shreFreeChgJob >> Complete
    








