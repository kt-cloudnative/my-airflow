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
                , WORKFLOW_NAME='engine-eoc-auBill-I',WORKFLOW_ID='9eaa96696fe34ff8846ba3e48307d770', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eoc-auBill-I-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 18, 9, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9eaa96696fe34ff8846ba3e48307d770')

    auBillJob_I_vol = []
    auBillJob_I_volMnt = []
    auBillJob_I_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_I_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_I_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_I_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_I_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_I = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'af860a60fc044286859962ac3429674e',
        'volumes': auBillJob_I_vol,
        'volume_mounts': auBillJob_I_volMnt,
        'env_from':auBillJob_I_env,
        'task_id':'auBillJob_I',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=I","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9eaa96696fe34ff8846ba3e48307d770')

    workflow = COMMON.getICISPipeline([
        authCheck,
        auBillJob_I,
        Complete
    ]) 

    # authCheck >> auBillJob_I >> Complete
    workflow








