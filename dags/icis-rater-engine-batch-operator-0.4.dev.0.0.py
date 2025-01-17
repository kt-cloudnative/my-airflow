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
                , WORKFLOW_NAME='engine-batch-operator',WORKFLOW_ID='81f0636eb6e04b0fb6e8ac2e5459e3dc', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-batch-operator-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 16, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('81f0636eb6e04b0fb6e8ac2e5459e3dc')

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
        'id' : 'cff028cfe06a4b2aaef48980decf626d',
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
        'id' : 'f6fb690cde834d36b006157ae5f4cf70',
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
        'id' : 'fa26b123afae4064bfcea1103f5204a5',
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
        'id' : 'a05a43506fc2422981caaebe8a486cf5',
        'task_id' : 'engineOperator',
        'method' : 'POST',
        'endpoint' : 'rest-gw-rater.dev.icis.kt.co.kr/hotbill/random/value',
        'headers' : {
    "Content-Type": "application/json"
},
        'data' : {
    "err": "true"
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
       
      

    Complete = COMMON.getICISCompleteWflowTask('81f0636eb6e04b0fb6e8ac2e5459e3dc')

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








