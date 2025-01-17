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
                , WORKFLOW_NAME='engine-eoc-auBill-All',WORKFLOW_ID='ce0a8610159f484d99e98c46b9648ff1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eoc-auBill-All-0.4.dev.1.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 14, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ce0a8610159f484d99e98c46b9648ff1')

    auBillJob_O_vol = []
    auBillJob_O_volMnt = []
    auBillJob_O_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_O_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_O_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_O_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_O_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_O = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '755551bf31c54297bd21948c98d917a1',
        'volumes': auBillJob_O_vol,
        'volume_mounts': auBillJob_O_volMnt,
        'env_from':auBillJob_O_env,
        'task_id':'auBillJob_O',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=O","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    auBillJob_I_vol = []
    auBillJob_I_volMnt = []
    auBillJob_I_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_I_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_I_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_I_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_I_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_I = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '16a96e209cd845d3b8b27b65da82b696',
        'volumes': auBillJob_I_vol,
        'volume_mounts': auBillJob_I_volMnt,
        'env_from':auBillJob_I_env,
        'task_id':'auBillJob_I',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=I","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    auBillJob_U_vol = []
    auBillJob_U_volMnt = []
    auBillJob_U_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_U_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_U_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_U_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_U_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_U = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '559ffadd7b9c48779598e67b10d3cd86',
        'volumes': auBillJob_U_vol,
        'volume_mounts': auBillJob_U_volMnt,
        'env_from':auBillJob_U_env,
        'task_id':'auBillJob_U',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=U","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    auBillJob_H_vol = []
    auBillJob_H_volMnt = []
    auBillJob_H_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    auBillJob_H_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    auBillJob_H_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    auBillJob_H_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    auBillJob_H_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    auBillJob_H = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b5601c7f09364a8c9f3cd1f3a76a1ce7',
        'volumes': auBillJob_H_vol,
        'volume_mounts': auBillJob_H_volMnt,
        'env_from':auBillJob_H_env,
        'task_id':'auBillJob_H',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=auBillJob","cyclYy=2024","cyclMonth=10","procType=H","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ce0a8610159f484d99e98c46b9648ff1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        auBillJob_I,
        auBillJob_U,
        auBillJob_H,
        auBillJob_O,
        Complete
    ]) 

    # authCheck >> auBillJob_I>>auBillJob_U>>auBillJob_H>>auBillJob_O >> Complete
    workflow








