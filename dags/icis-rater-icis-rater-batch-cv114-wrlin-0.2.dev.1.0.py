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
    'dag_id':'icis-rater-icis-rater-batch-cv114-wrlin-0.2.dev.1.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('1bc45ae7e8454a54a814347ed6d17d59')

    cv114WrlinJob_vol = []
    cv114WrlinJob_volMnt = []
    cv114WrlinJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cv114WrlinJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cv114WrlinJob_env = [getICISConfigMap('icis-rater-batch-cv114-configmap'), getICISConfigMap('icis-rater-batch-cv114-configmap2'), getICISSecret('icis-rater-batch-cv114-secret')]
    cv114WrlinJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cv114WrlinJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cv114WrlinJob = COMMON.getICISKubernetesPodOperator({
        'id' : '0cac1b2a54354e558a895500ba902dcb',
        'volumes': cv114WrlinJob_vol,
        'volume_mounts': cv114WrlinJob_volMnt,
        'env_from':cv114WrlinJob_env,
        'task_id':'cv114WrlinJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv114:20231108171343',
        'arguments':["--job.names=cv114WrlinJob", "runType=T", "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('1bc45ae7e8454a54a814347ed6d17d59')

    authCheck >> cv114WrlinJob >> Complete
    








