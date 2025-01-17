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
    'dag_id':'icis-rater-icis-rater-batch-tarif-0.1.dev.6.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 10, 13, 17, 58, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('e4c994fd4d8140b98b9a4168374d9bcf')

    l7McidTarifJob_vol = []
    l7McidTarifJob_volMnt = []
    l7McidTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    l7McidTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    l7McidTarifJob_env = [getICISConfigMap('icis-rater-batch-tarif-configmap'), getICISConfigMap('icis-rater-batch-tarif-configmap2'), getICISSecret('icis-rater-batch-tarif-secret')]
    l7McidTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    l7McidTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    l7McidTarifJob = COMMON.getICISKubernetesPodOperator({
        'id' : '08907a2ed1e143a6b1fdcff0b8340b57',
        'volumes': l7McidTarifJob_vol,
        'volume_mounts': l7McidTarifJob_volMnt,
        'env_from':l7McidTarifJob_env,
        'task_id':'l7McidTarifJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-tarif:20231013175154',
        'arguments':["--job.names=l7McidTarifJob", "runType=T" , "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('e4c994fd4d8140b98b9a4168374d9bcf')

    authCheck >> l7McidTarifJob >> Complete
    








