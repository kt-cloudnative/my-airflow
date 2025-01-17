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

from icis_common_rater_temp import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis_batch_rds-0.3.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 15, 0, 5, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('2cc62d9a202b4dc29e9474c12489c611')

    rdsSetlJob_vol = []
    rdsSetlJob_volMnt = []
    rdsSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    rdsSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    rdsSetlJob_env = [getICISConfigMap('icis-rater-batch-rds-configmap'), getICISConfigMap('icis-rater-batch-rds-configmap2'), getICISSecret('icis-rater-batch-rds-secret')]
    rdsSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    rdsSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    rdsSetlJob = COMMON.getICISKubernetesPodOperator({
        'id' : '73a7d4883eb24c84b34ccc7bc467dbaa',
        'volumes': rdsSetlJob_vol,
        'volume_mounts': rdsSetlJob_volMnt,
        'env_from':rdsSetlJob_env,
        'task_id':'rdsSetlJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-rds:20231115101521',
        'arguments':["--job.names=rdsSetlJob", "billMonth=202312"]
    })


    Complete = getICISCompleteWflowTask('2cc62d9a202b4dc29e9474c12489c611')

    authCheck >> rdsSetlJob >> Complete
    








