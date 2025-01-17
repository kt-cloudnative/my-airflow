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
    'dag_id':'icis-rater-icis-rater-batch-cvrepno-0.2.dev.2.0',
    'schedule_interval':'@once',
    'start_date': datetime(2023, 9, 18, 1, 4, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('06e2be5d404149d384b9b90965d8db95')

    CvRepNoWrlinCvFileCretJob_vol = []
    CvRepNoWrlinCvFileCretJob_volMnt = []
    CvRepNoWrlinCvFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    CvRepNoWrlinCvFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    CvRepNoWrlinCvFileCretJob_env = [getICISConfigMap('icis-rater-batch-cvrepno-configmap'), getICISConfigMap('icis-rater-batch-cvrepno-configmap2'), getICISSecret('icis-rater-batch-cvrepno-secret')]
    CvRepNoWrlinCvFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    CvRepNoWrlinCvFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    CvRepNoWrlinCvFileCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : '0f51276561ae4d3c83a39cb98391ed83',
        'volumes': CvRepNoWrlinCvFileCretJob_vol,
        'volume_mounts': CvRepNoWrlinCvFileCretJob_volMnt,
        'env_from':CvRepNoWrlinCvFileCretJob_env,
        'task_id':'CvRepNoWrlinCvFileCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cvrepno:20230919161908',
        'arguments':["--job.names=cvRepNoWrlinJob","runType=T","useYm=202307"]
    })


    Complete = getICISCompleteWflowTask('06e2be5d404149d384b9b90965d8db95')

    authCheck >> CvRepNoWrlinCvFileCretJob >> Complete
    








