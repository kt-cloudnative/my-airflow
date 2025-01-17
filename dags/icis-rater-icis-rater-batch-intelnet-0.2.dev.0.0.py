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
    'dag_id':'icis-rater-icis-rater-batch-intelnet-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('e968cb881fa544058f3990979142d754')

    intelnetCsgnFeeJob_vol = []
    intelnetCsgnFeeJob_volMnt = []
    intelnetCsgnFeeJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intelnetCsgnFeeJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intelnetCsgnFeeJob_env = [getICISConfigMap('icis-rater-batch-intelnet-configmap'), getICISConfigMap('icis-rater-batch-intelnet-configmap2'), getICISSecret('icis-rater-batch-intelnet-secret')]
    intelnetCsgnFeeJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intelnetCsgnFeeJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intelnetCsgnFeeJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'a91cff542a8e45d189bf6ab61ba59909',
        'volumes': intelnetCsgnFeeJob_vol,
        'volume_mounts': intelnetCsgnFeeJob_volMnt,
        'env_from':intelnetCsgnFeeJob_env,
        'task_id':'intelnetCsgnFeeJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-intelnet:20231107134232',
        'arguments':["--job.names=intelnetCsgnFeeJob", "runType=T", "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('e968cb881fa544058f3990979142d754')

    authCheck >> intelnetCsgnFeeJob >> Complete
    








