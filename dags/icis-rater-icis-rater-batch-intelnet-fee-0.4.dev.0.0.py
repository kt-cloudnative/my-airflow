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

from icis_common import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater'
                , WORKFLOW_NAME='icis-rater-batch-intelnet-fee',WORKFLOW_ID='572e827bc9bb43d49def554b10830b06', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intelnet-fee-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 31, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('572e827bc9bb43d49def554b10830b06')

    intelnetCsgnFeeJob_vol = []
    intelnetCsgnFeeJob_volMnt = []
    intelnetCsgnFeeJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intelnetCsgnFeeJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intelnetCsgnFeeJob_env = [getICISConfigMap('icis-rater-batch-intelnet-configmap'), getICISConfigMap('icis-rater-batch-intelnet-configmap2'), getICISSecret('icis-rater-batch-intelnet-secret')]
    intelnetCsgnFeeJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intelnetCsgnFeeJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intelnetCsgnFeeJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '22dd26a9ee234df0a092b5d88f048cb9',
        'volumes': intelnetCsgnFeeJob_vol,
        'volume_mounts': intelnetCsgnFeeJob_volMnt,
        'env_from':intelnetCsgnFeeJob_env,
        'task_id':'intelnetCsgnFeeJob',
        'image':'/icis/icis-rater-batch-intelnet:20240801101843',
        'arguments':["--job.names=intelnetCsgnFeeJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('572e827bc9bb43d49def554b10830b06')

    authCheck >> intelnetCsgnFeeJob >> Complete
    








