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
    'dag_id':'icis-rater-icis-rater-batch-cv060-voip-info-0.2.dev.2.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('e14d841f42314d7f9edff8001fc3ea58')

    voipInfoChageJob_vol = []
    voipInfoChageJob_volMnt = []
    voipInfoChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    voipInfoChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    voipInfoChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    voipInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    voipInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    voipInfoChageJob = COMMON.getICISKubernetesPodOperator({
        'id' : '44c62842533c4335a3ad557d1d3f9f14',
        'volumes': voipInfoChageJob_vol,
        'volume_mounts': voipInfoChageJob_volMnt,
        'env_from':voipInfoChageJob_env,
        'task_id':'voipInfoChageJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv060:20231121101400',
        'arguments':["--job.names=voipInfoChageJob","runType=T","useYm=202311"]
    })


    Complete = getICISCompleteWflowTask('e14d841f42314d7f9edff8001fc3ea58')

    authCheck >> voipInfoChageJob >> Complete
    








