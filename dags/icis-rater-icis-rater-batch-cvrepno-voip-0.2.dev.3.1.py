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
    'dag_id':'icis-rater-icis-rater-batch-cvrepno-voip-0.2.dev.3.1',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('d6064b7e84cb4dabb93056b8b0fd20ae')

    cvRepNoVoIPJob_vol = []
    cvRepNoVoIPJob_volMnt = []
    cvRepNoVoIPJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvRepNoVoIPJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvRepNoVoIPJob_env = [getICISConfigMap('icis-rater-batch-cvrepno-configmap'), getICISConfigMap('icis-rater-batch-cvrepno-configmap2'), getICISSecret('icis-rater-batch-cvrepno-secret')]
    cvRepNoVoIPJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvRepNoVoIPJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvRepNoVoIPJob = COMMON.getICISKubernetesPodOperator({
        'id' : '89831eea9f9d4886b4f9bf8f8c08716a',
        'volumes': cvRepNoVoIPJob_vol,
        'volume_mounts': cvRepNoVoIPJob_volMnt,
        'env_from':cvRepNoVoIPJob_env,
        'task_id':'cvRepNoVoIPJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvrepno:20240401142152',
        'arguments':["--job.names=cvRepNoVoIPJob", "runType=T", "cyclYm=202306"]
    })


    Complete = getICISCompleteWflowTask('d6064b7e84cb4dabb93056b8b0fd20ae')

    authCheck >> cvRepNoVoIPJob >> Complete
    








