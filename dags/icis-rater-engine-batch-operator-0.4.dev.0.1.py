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
                , WORKFLOW_NAME='engine-batch-operator',WORKFLOW_ID='9925eafa7abf4c4aafc00c313cd14dfa', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-batch-operator-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 16, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9925eafa7abf4c4aafc00c313cd14dfa')

    engineEom_vol = []
    engineEom_volMnt = []
    engineEom_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    engineEom_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    engineEom_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    engineEom_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    engineEom_env = [getICISConfigMap('icis-rater-engine-eom-batch-configmap'), getICISConfigMap('icis-rater-engine-eom-batch-configmap2'), getICISSecret('icis-rater-engine-eom-batch-secret')]
    engineEom_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    engineEom_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    engineEom = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c53807390a6d40ecaf45540de8095653',
        'volumes': engineEom_vol,
        'volume_mounts': engineEom_volMnt,
        'env_from':engineEom_env,
        'task_id':'engineEom',
        'image':'/icis/icis-rater-engine-eom-batch:20240229110650',
        'arguments':["--job.names=eomMainJob", "cyclYy=${YYYY,DD,+1}", "cyclMonth=${MM,DD,+1}", "run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    engineEoc_vol = []
    engineEoc_volMnt = []
    engineEoc_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    engineEoc_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    engineEoc_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    engineEoc_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    engineEoc_env = [getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap2'), getICISSecret('icis-rater-engine-eoc-batch-secret')]
    engineEoc_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    engineEoc_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    engineEoc = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'dfb01b5a754b4ba0b82d12f76d8ca9a6',
        'volumes': engineEoc_vol,
        'volume_mounts': engineEoc_volMnt,
        'env_from':engineEoc_env,
        'task_id':'engineEoc',
        'image':'/icis/icis-rater-engine-eoc-batch:20240229110815',
        'arguments':["--job.names=eocSubJob", "cyclYy=${YYYY,DD,-1}", "cyclMonth=${MM,DD,-1}", "run.id=${YYYYMMDDHHMISS}", "--HIKARI-POOL-SIZE=10"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    engineEocSub_vol = []
    engineEocSub_volMnt = []
    engineEocSub_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    engineEocSub_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    engineEocSub_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    engineEocSub_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    engineEocSub_env = [getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap2'), getICISSecret('icis-rater-engine-eoc-batch-secret')]
    engineEocSub_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    engineEocSub_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    engineEocSub = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'de0e1ef75047410598110e76ce690ddd',
        'volumes': engineEocSub_vol,
        'volume_mounts': engineEocSub_volMnt,
        'env_from':engineEocSub_env,
        'task_id':'engineEocSub',
        'image':'/icis/icis-rater-engine-eoc-batch:20240729164450',
        'arguments':["--job.names=eocSubJob", "cyclYy=${YYYY,DD,-1}", "cyclMonth=${MM,DD,-1}", "run.id=${YYYYMMDDHHMISS}", "--HIKARI-POOL-SIZE=10"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


    engineOperator = COMMON.getICISSimpleHttpOperator_v1({
        'id' : '63424f9a33874f42b52a46d59a6b0091',
        'task_id' : 'engineOperator',
        'method' : 'POST',
        'endpoint' : 'rest-gw-rater.dev.icis.kt.co.kr/hotbill/random/value',
        'headers' : {
    "Content-Type": "application/json"
},
        'data' : {
    "err": "false"
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9925eafa7abf4c4aafc00c313cd14dfa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        engineOperator,
        engineEoc,
        engineEocSub,
        engineEom,
        Complete
    ]) 

    # authCheck >> engineOperator >> engineEoc >> engineEocSub >> engineEom >> Complete
    workflow








