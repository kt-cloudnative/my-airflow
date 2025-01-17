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
                , WORKFLOW_NAME='engine-eoc-auBill-U',WORKFLOW_ID='a01afc5d649d4e29817a9b45d7033a24', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eoc-auBill-U-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 18, 12, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a01afc5d649d4e29817a9b45d7033a24')

    auBillJob_U_vol = []
    auBillJob_U_volMnt = []
    auBillJob_U_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_U_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_U_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_U_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_U_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_U = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7b2d7b56988443e88b2ca1c20e91f628',
        'volumes': auBillJob_U_vol,
        'volume_mounts': auBillJob_U_volMnt,
        'env_from':auBillJob_U_env,
        'task_id':'auBillJob_U',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=U","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a01afc5d649d4e29817a9b45d7033a24')

    workflow = COMMON.getICISPipeline([
        authCheck,
        auBillJob_U,
        Complete
    ]) 

    # authCheck >> auBillJob_U >> Complete
    workflow








