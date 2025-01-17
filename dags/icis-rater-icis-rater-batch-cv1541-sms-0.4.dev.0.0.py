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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-sms',WORKFLOW_ID='45f9899d93cd4d4cac42c5cb3a3b62fa', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-sms-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 4, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('45f9899d93cd4d4cac42c5cb3a3b62fa')

    chageNtcSmsJob_vol = []
    chageNtcSmsJob_volMnt = []
    chageNtcSmsJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    chageNtcSmsJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    chageNtcSmsJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    chageNtcSmsJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    chageNtcSmsJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    chageNtcSmsJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1a0b2c04633744628942b928e8c6980e',
        'volumes': chageNtcSmsJob_vol,
        'volume_mounts': chageNtcSmsJob_volMnt,
        'env_from':chageNtcSmsJob_env,
        'task_id':'chageNtcSmsJob',
        'image':'/icis/icis-rater-batch-cv1541:20240905144237',
        'arguments':["--job.names=chageNtcSmsJob", "cyclYm=202403", "cyclYmd=20240221"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('45f9899d93cd4d4cac42c5cb3a3b62fa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        chageNtcSmsJob,
        Complete
    ]) 

    # authCheck >> chageNtcSmsJob >> Complete
    workflow








