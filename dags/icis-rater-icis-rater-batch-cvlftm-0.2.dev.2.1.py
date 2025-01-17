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
    'dag_id':'icis-rater-icis-rater-batch-cvlftm-0.2.dev.2.1',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('703197db0b8b4e2e948ededf368b23ac')

    cvLftmNoJob_vol = []
    cvLftmNoJob_volMnt = []
    cvLftmNoJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvLftmNoJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvLftmNoJob_env = [getICISConfigMap('icis-rater-batch-cvlftm-configmap'), getICISConfigMap('icis-rater-batch-cvlftm-configmap2'), getICISSecret('icis-rater-batch-cvlftm-secret')]
    cvLftmNoJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvLftmNoJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvLftmNoJob = COMMON.getICISKubernetesPodOperator({
        'id' : '6a92557723144297adc61e2838cd7266',
        'volumes': cvLftmNoJob_vol,
        'volume_mounts': cvLftmNoJob_volMnt,
        'env_from':cvLftmNoJob_env,
        'task_id':'cvLftmNoJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvlftm:20240329154813',
        'arguments':["--job.names=cvLftmNoJob", "runType=T", "cyclYm=202306"]
    })


    Complete = getICISCompleteWflowTask('703197db0b8b4e2e948ededf368b23ac')

    authCheck >> cvLftmNoJob >> Complete
    








