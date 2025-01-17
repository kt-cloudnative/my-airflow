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
    'dag_id':'icis-rater-icis-rater-batch-lorrg-0.1.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 7, 13, 12, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('2fd49285856847f98128b9bf659f4712')

    lnpOrrgCretJob_vol = []
    lnpOrrgCretJob_volMnt = []
    lnpOrrgCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    lnpOrrgCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    lnpOrrgCretJob_env = [getICISConfigMap('icis-rater-batch-lorrg-configmap'), getICISConfigMap('icis-rater-batch-lorrg-configmap2'), getICISSecret('icis-rater-batch-lorrg-secret')]
    lnpOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lnpOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lnpOrrgCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'f5a92c424c844144b2c2291421482f3f',
        'volumes': lnpOrrgCretJob_vol,
        'volume_mounts': lnpOrrgCretJob_volMnt,
        'env_from':lnpOrrgCretJob_env,
        'task_id':'lnpOrrgCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-lorrg:20230714175029',
        'arguments':["--job.names=lnpOrrgCretJob"]
    })


    Complete = getICISCompleteWflowTask('2fd49285856847f98128b9bf659f4712')

    authCheck >> lnpOrrgCretJob >> Complete
    








