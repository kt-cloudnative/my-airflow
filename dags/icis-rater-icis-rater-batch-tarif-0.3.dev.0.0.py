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
    'dag_id':'icis-rater-icis-rater-batch-tarif-0.3.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2024, 1, 22, 15, 20, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('c45134fd188c420696d1fdbf30fb7f7a')

    keSmsTarifJob_vol = []
    keSmsTarifJob_volMnt = []
    keSmsTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    keSmsTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    keSmsTarifJob_env = [getICISConfigMap('icis-rater-batch-tarif-configmap'), getICISConfigMap('icis-rater-batch-tarif-configmap2'), getICISSecret('icis-rater-batch-tarif-secret')]
    keSmsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    keSmsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    keSmsTarifJob = COMMON.getICISKubernetesPodOperator({
        'id' : '56c08b41f3cd4a9087833a00dd8f5dca',
        'volumes': keSmsTarifJob_vol,
        'volume_mounts': keSmsTarifJob_volMnt,
        'env_from':keSmsTarifJob_env,
        'task_id':'keSmsTarifJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-tarif:20240122144356',
        'arguments':["--job.names=keSmsTarifJob","runType=T", "cyclYm=202311"]
    })


    Complete = getICISCompleteWflowTask('c45134fd188c420696d1fdbf30fb7f7a')

    authCheck >> keSmsTarifJob >> Complete
    








