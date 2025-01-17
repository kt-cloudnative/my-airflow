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
                , WORKFLOW_NAME='icis-rater-batch-connfee-del',WORKFLOW_ID='8e23dfe574044d4094f72ee6c026795b', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-connfee-del-0.4.dev.1.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 13, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8e23dfe574044d4094f72ee6c026795b')

    connfeeDelJob_vol = []
    connfeeDelJob_volMnt = []
    connfeeDelJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    connfeeDelJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    connfeeDelJob_env = [getICISConfigMap('icis-rater-batch-connfee-configmap'), getICISConfigMap('icis-rater-batch-connfee-configmap2'), getICISSecret('icis-rater-batch-connfee-secret')]
    connfeeDelJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    connfeeDelJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    connfeeDelJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2ed9c882dde24448920baaf2e7528528',
        'volumes': connfeeDelJob_vol,
        'volume_mounts': connfeeDelJob_volMnt,
        'env_from':connfeeDelJob_env,
        'task_id':'connfeeDelJob',
        'image':'/icis/icis-rater-batch-connfee:20240813162708',
        'arguments':["--job.names=connfeeDelJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8e23dfe574044d4094f72ee6c026795b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        connfeeDelJob,
        Complete
    ]) 

    # authCheck >> connfeeDelJob >> Complete
    workflow








