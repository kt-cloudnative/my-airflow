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
    'dag_id':'icis-rater-icis-rater-batch-cv060-wrlin-tlk-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('cb4361bc69e2491fa95c6cf9e593868d')

    wrlinTlkChageJob_vol = []
    wrlinTlkChageJob_volMnt = []
    wrlinTlkChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    wrlinTlkChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    wrlinTlkChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    wrlinTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    wrlinTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    wrlinTlkChageJob = COMMON.getICISKubernetesPodOperator({
        'id' : '5fba067ce08340cfa615b4b7f952427b',
        'volumes': wrlinTlkChageJob_vol,
        'volume_mounts': wrlinTlkChageJob_volMnt,
        'env_from':wrlinTlkChageJob_env,
        'task_id':'wrlinTlkChageJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv060:20231107162613',
        'arguments':["--job.names=wrlinTlkChageJob","runType=T","useYm=202311"]
    })


    Complete = getICISCompleteWflowTask('cb4361bc69e2491fa95c6cf9e593868d')

    authCheck >> wrlinTlkChageJob >> Complete
    








