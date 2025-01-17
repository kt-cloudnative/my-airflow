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
    'dag_id':'icis-rater-icis-rater-batch-tarif-l2-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 8, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('a00e4d44a897423989b4b540c26db5ad')

    l2SmsTarifJob_vol = []
    l2SmsTarifJob_volMnt = []
    l2SmsTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    l2SmsTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    l2SmsTarifJob_env = [getICISConfigMap('icis-rater-batch-tarif-configmap'), getICISConfigMap('icis-rater-batch-tarif-configmap2'), getICISSecret('icis-rater-batch-tarif-secret')]
    l2SmsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    l2SmsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    l2SmsTarifJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'b49185f7afab4147bd8ef528a8b42f24',
        'volumes': l2SmsTarifJob_vol,
        'volume_mounts': l2SmsTarifJob_volMnt,
        'env_from':l2SmsTarifJob_env,
        'task_id':'l2SmsTarifJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-tarif:20231108190856',
        'arguments':["--job.names=l2SmsTarifJob", "runType=T" , "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('a00e4d44a897423989b4b540c26db5ad')

    authCheck >> l2SmsTarifJob >> Complete
    








