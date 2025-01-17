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
                , WORKFLOW_NAME='engine-eoc-auBill-O',WORKFLOW_ID='b0860d2fc6ac4217a40f01f51f7c77cd', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eoc-auBill-O-0.0.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 18, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b0860d2fc6ac4217a40f01f51f7c77cd')

    auBillJob_O_vol = []
    auBillJob_O_volMnt = []
    auBillJob_O_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_O_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_O_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_O_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_O_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_O = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '558304f301184202bf4c15fcdd998537',
        'volumes': auBillJob_O_vol,
        'volume_mounts': auBillJob_O_volMnt,
        'env_from':auBillJob_O_env,
        'task_id':'auBillJob_O',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=O","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b0860d2fc6ac4217a40f01f51f7c77cd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        auBillJob_O,
        Complete
    ]) 

    # authCheck >> auBillJob_O >> Complete
    workflow








