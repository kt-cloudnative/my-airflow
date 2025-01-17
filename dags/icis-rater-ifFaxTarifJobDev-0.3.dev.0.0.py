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
    'dag_id':'icis-rater-ifFaxTarifJobDev-0.3.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2024, 1, 29, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('fb3eea63ed1a46adb422590d22782fe9')

    ifFaxTarifJob_vol = []
    ifFaxTarifJob_volMnt = []
    ifFaxTarifJob_env = [getICISConfigMap('icis-rater-batch-tarif-configmap'), getICISConfigMap('icis-rater-batch-tarif-configmap2'), getICISSecret('icis-rater-batch-tarif-secret')]
    ifFaxTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ifFaxTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ifFaxTarifJob = COMMON.getICISKubernetesPodOperator({
        'id' : '1c77ea9694524223b98961e8b883d831',
        'volumes': ifFaxTarifJob_vol,
        'volume_mounts': ifFaxTarifJob_volMnt,
        'env_from':ifFaxTarifJob_env,
        'task_id':'ifFaxTarifJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-tarif:20240129140604',
        'arguments':["--job.names=ifFaxTarifJob", "runType=T", "cyclYm=202311" 
]
    })


    Complete = getICISCompleteWflowTask('fb3eea63ed1a46adb422590d22782fe9')

    authCheck >> ifFaxTarifJob >> Complete
    








