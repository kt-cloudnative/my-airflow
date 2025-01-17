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
                , WORKFLOW_NAME='icis-rater-batch-lmtchk',WORKFLOW_ID='2524cdcd079a4671a53c137a90abb1ad', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-lmtchk-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 1, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2524cdcd079a4671a53c137a90abb1ad')

    lmtAmtFileCretJob_vol = []
    lmtAmtFileCretJob_volMnt = []
    lmtAmtFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lmtAmtFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lmtAmtFileCretJob_env = [getICISConfigMap('icis-rater-batch-lmtchk-configmap'), getICISConfigMap('icis-rater-batch-lmtchk-configmap2'), getICISSecret('icis-rater-batch-lmtchk-secret')]
    lmtAmtFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lmtAmtFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lmtAmtFileCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cfaab6d4a889420a8b4a54f14a4e3b47',
        'volumes': lmtAmtFileCretJob_vol,
        'volume_mounts': lmtAmtFileCretJob_volMnt,
        'env_from':lmtAmtFileCretJob_env,
        'task_id':'lmtAmtFileCretJob',
        'image':'/icis/icis-rater-batch-lmtchk:20240731172240',
        'arguments':["--job.names=lmtAmtExcsFileJob", "runType=T", "cyclYm=202404", "clstrDt=20240401"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2524cdcd079a4671a53c137a90abb1ad')

    authCheck >> lmtAmtFileCretJob >> Complete
    








