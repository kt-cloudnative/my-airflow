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
                , WORKFLOW_NAME='kdapUsNoBillDayGrp1',WORKFLOW_ID='4fa9f3f73ee24d68977ea5911a7cb8f8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-kdapUsNoBillDayGrp1-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 12, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4fa9f3f73ee24d68977ea5911a7cb8f8')

    kdapUsNoBillJob_vol = []
    kdapUsNoBillJob_volMnt = []
    kdapUsNoBillJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kdapUsNoBillJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kdapUsNoBillJob_env = [getICISConfigMap('icis-rater-batch-kdap-configmap'), getICISConfigMap('icis-rater-batch-kdap-configmap2'), getICISSecret('icis-rater-batch-kdap-secret')]
    kdapUsNoBillJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kdapUsNoBillJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kdapUsNoBillJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '69a945a214154bff9369299c50ba81ca',
        'volumes': kdapUsNoBillJob_vol,
        'volume_mounts': kdapUsNoBillJob_volMnt,
        'env_from':kdapUsNoBillJob_env,
        'task_id':'kdapUsNoBillJob',
        'image':'/icis/icis-rater-batch-kdap:0.4.0.16',
        'arguments':["--job.names=kdapUsNoBillJob", "retvType=D", "cyclYm=20240901",  "cyclGroup=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4fa9f3f73ee24d68977ea5911a7cb8f8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kdapUsNoBillJob,
        Complete
    ]) 

    # authCheck >> kdapUsNoBillJob >> Complete
    workflow








