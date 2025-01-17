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
    'dag_id':'icis-rater-icis-rater-batch-cv060-wrlin-info-0.2.dev.0.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('f501bb9554b849eca5c63b2b1dbbdec1')

    wrlinInfoChageJob_vol = []
    wrlinInfoChageJob_volMnt = []
    wrlinInfoChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    wrlinInfoChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    wrlinInfoChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    wrlinInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    wrlinInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    wrlinInfoChageJob = COMMON.getICISKubernetesPodOperator({
        'id' : '7cffb69b060c4386a43ffdce05cc203f',
        'volumes': wrlinInfoChageJob_vol,
        'volume_mounts': wrlinInfoChageJob_volMnt,
        'env_from':wrlinInfoChageJob_env,
        'task_id':'wrlinInfoChageJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cv060:20231107162613',
        'arguments':["--job.names=wrlinInfoChageJob","runType=T","useYm=202311"]
    })


    Complete = getICISCompleteWflowTask('f501bb9554b849eca5c63b2b1dbbdec1')

    authCheck >> wrlinInfoChageJob >> Complete
    








