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
                , WORKFLOW_NAME='icis_rater_rerate_1571_rebill',WORKFLOW_ID='9c6fb3b1e3c24497ba491a8cd8a90fe4', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis_rater_rerate_1571_rebill-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9c6fb3b1e3c24497ba491a8cd8a90fe4')

    rs1571ChgJob_vol = []
    rs1571ChgJob_volMnt = []
    rs1571ChgJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    rs1571ChgJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    rs1571ChgJob_env = [getICISConfigMap('icis-rater-batch-rerate-configmap'), getICISConfigMap('icis-rater-batch-rerate-configmap2'), getICISSecret('icis-rater-batch-rerate-secret')]
    rs1571ChgJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    rs1571ChgJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    rs1571ChgJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a48135e8dd9f43f2a1bfc3ade3b94ce0',
        'volumes': rs1571ChgJob_vol,
        'volume_mounts': rs1571ChgJob_volMnt,
        'env_from':rs1571ChgJob_env,
        'task_id':'rs1571ChgJob',
        'image':'/icis/icis-rater-batch-rerate:0.4.0.1',
        'arguments':["--job.names=rs1571ChgJob", "runType=T", "cyclYm=202410"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9c6fb3b1e3c24497ba491a8cd8a90fe4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        rs1571ChgJob,
        Complete
    ]) 

    # authCheck >> rs1571ChgJob >> Complete
    workflow








