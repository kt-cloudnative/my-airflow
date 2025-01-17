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
    'dag_id':'icis-rater-icis-rater-batch-cvrepno-wrlin-0.2.dev.1.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('823ac3b46f56419d8a6c0c2bef7892c5')

    cvRepNoWrlinJob_vol = []
    cvRepNoWrlinJob_volMnt = []
    cvRepNoWrlinJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvRepNoWrlinJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvRepNoWrlinJob_env = [getICISConfigMap('icis-rater-batch-cvrepno-configmap'), getICISConfigMap('icis-rater-batch-cvrepno-configmap2'), getICISSecret('icis-rater-batch-cvrepno-secret')]
    cvRepNoWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvRepNoWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvRepNoWrlinJob = COMMON.getICISKubernetesPodOperator({
        'id' : '36cbe7f0e1594cf5bc322f1169c6fb52',
        'volumes': cvRepNoWrlinJob_vol,
        'volume_mounts': cvRepNoWrlinJob_volMnt,
        'env_from':cvRepNoWrlinJob_env,
        'task_id':'cvRepNoWrlinJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvrepno:20231108105050',
        'arguments':["--job.names=cvRepNoWrlinJob", "runType=T", "useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('823ac3b46f56419d8a6c0c2bef7892c5')

    authCheck >> cvRepNoWrlinJob >> Complete
    








