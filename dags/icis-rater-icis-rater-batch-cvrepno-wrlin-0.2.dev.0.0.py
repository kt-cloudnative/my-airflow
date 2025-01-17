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
    'dag_id':'icis-rater-icis-rater-batch-cvrepno-wrlin-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('4db0c839e21c4c3ab522e66e2336a1b4')

    cvRepNoWrlinJob_vol = []
    cvRepNoWrlinJob_volMnt = []
    cvRepNoWrlinJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvRepNoWrlinJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvRepNoWrlinJob_env = [getICISConfigMap('icis-rater-batch-cvrepno-configmap'), getICISConfigMap('icis-rater-batch-cvrepno-configmap2'), getICISSecret('icis-rater-batch-cvrepno-secret')]
    cvRepNoWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvRepNoWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvRepNoWrlinJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'c87aa4a9d75241beaa3c69f971f1dadb',
        'volumes': cvRepNoWrlinJob_vol,
        'volume_mounts': cvRepNoWrlinJob_volMnt,
        'env_from':cvRepNoWrlinJob_env,
        'task_id':'cvRepNoWrlinJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvrepno:20231107180810',
        'arguments':["--job.names=cvRepNoWrlinJob", "runType=T", "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('4db0c839e21c4c3ab522e66e2336a1b4')

    authCheck >> cvRepNoWrlinJob >> Complete
    








