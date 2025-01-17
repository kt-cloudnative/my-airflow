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
    'dag_id':'icis-rater-icis-rater-batch-inmar-0.1.dev.0.0',
    'schedule_interval':'0 13 * * *',
    'start_date': datetime(2023, 7, 13, 13, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('9c50b88029cc4a8c884929655d493e68')

    inmarTgtCretJob_vol = []
    inmarTgtCretJob_volMnt = []
    inmarTgtCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    inmarTgtCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    inmarTgtCretJob_env = [getICISConfigMap('icis-rater-batch-inmar-configmap'),getICISConfigMap('icis-rater-batch-inmar-configmap2'), getICISSecret('icis-rater-batch-inmar-secret')]
    inmarTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    inmarTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    inmarTgtCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'dc4e81286ab142d3b8164970354a6089',
        'volumes': inmarTgtCretJob_vol,
        'volume_mounts': inmarTgtCretJob_volMnt,
        'env_from':inmarTgtCretJob_env,
        'task_id':'inmarTgtCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-inmar:0.1.0.8',
        'arguments':["--job.names=inmarTgtCretJob" , "procDate=20231106"]
    })


    Complete = getICISCompleteWflowTask('9c50b88029cc4a8c884929655d493e68')

    authCheck >> inmarTgtCretJob >> Complete
    








