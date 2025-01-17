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
    'dag_id':'icis-rater-icis-rater-batch-cv1541-wrlin-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('4c6d3ceb13284f3181bf2ff3fd4daf98')

    cvWrlinJob_vol = []
    cvWrlinJob_volMnt = []
    cvWrlinJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvWrlinJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvWrlinJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvWrlinJob = COMMON.getICISKubernetesPodOperator({
        'id' : 'dc190468d17746e6aa7918678bbe1ecc',
        'volumes': cvWrlinJob_vol,
        'volume_mounts': cvWrlinJob_volMnt,
        'env_from':cvWrlinJob_env,
        'task_id':'cvWrlinJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv1541:20231107162618',
        'arguments':["--job.names=cvWrlinJob","runType=T","useYm=202306"]
    })


    Complete = getICISCompleteWflowTask('4c6d3ceb13284f3181bf2ff3fd4daf98')

    authCheck >> cvWrlinJob >> Complete
    








