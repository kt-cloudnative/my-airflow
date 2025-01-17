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
                , WORKFLOW_NAME='cdr-batch',WORKFLOW_ID='e8fb2a249d404fc0a0b1f5434d507568', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-cdr-batch-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 29, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e8fb2a249d404fc0a0b1f5434d507568')

    ldinCdrJob_vol = []
    ldinCdrJob_volMnt = []
    ldinCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ldinCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch-data/'))

    ldinCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ldinCdrJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '52e640bea83a41138e22718b6990f6e3',
        'volumes': ldinCdrJob_vol,
        'volume_mounts': ldinCdrJob_volMnt,
        'env_from':ldinCdrJob_env,
        'task_id':'ldinCdrJob',
        'image':'/icis/icis-rater-batch-cdr:0.4.0.7',
        'arguments':["--job.names=ldinCdrJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cdrCretJob_vol = []
    cdrCretJob_volMnt = []
    cdrCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater-data/'))

    cdrCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch-data/'))

    cdrCretJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cdrCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5136ef5b02f64eeab349b44a0a9f4481',
        'volumes': cdrCretJob_vol,
        'volume_mounts': cdrCretJob_volMnt,
        'env_from':cdrCretJob_env,
        'task_id':'cdrCretJob',
        'image':'/icis/icis-rater-batch-cdr:0.4.0.7',
        'arguments':["--job.names=cdrCretJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    drotCdrJob_vol = []
    drotCdrJob_volMnt = []
    drotCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    drotCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch-data/'))

    drotCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    drotCdrJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8b5323ca299145aead86945bc8bbda4d',
        'volumes': drotCdrJob_vol,
        'volume_mounts': drotCdrJob_volMnt,
        'env_from':drotCdrJob_env,
        'task_id':'drotCdrJob',
        'image':'/icis/icis-rater-batch-cdr:0.4.0.7',
        'arguments':["--job.names=drotCdrJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e8fb2a249d404fc0a0b1f5434d507568')

    authCheck >> ldinCdrJob>>drotCdrJob>>cdrCretJob >> Complete
    








