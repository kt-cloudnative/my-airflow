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
                , WORKFLOW_NAME='cdr_batch_20240729',WORKFLOW_ID='32a8058e29af4e9e990584cb701decc6', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-cdr_batch_20240729-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 29, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('32a8058e29af4e9e990584cb701decc6')

    ldinCdrJob_vol = []
    ldinCdrJob_volMnt = []
    ldinCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ldinCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    ldinCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ldinCdrJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '47af1d06df6644479c67850914f03c32',
        'volumes': ldinCdrJob_vol,
        'volume_mounts': ldinCdrJob_volMnt,
        'env_from':ldinCdrJob_env,
        'task_id':'ldinCdrJob',
        'image':'/icis/icis-rater-batch-cdr:20240729142001',
        'arguments':["--job.names=ldinCdrJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cdrCretJob_vol = []
    cdrCretJob_volMnt = []
    cdrCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_date/'))

    cdrCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cdrCretJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cdrCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '440f26e060ae4f7997ca58e231dca0f6',
        'volumes': cdrCretJob_vol,
        'volume_mounts': cdrCretJob_volMnt,
        'env_from':cdrCretJob_env,
        'task_id':'cdrCretJob',
        'image':'/icis/icis-rater-batch-cdr:20240729142001',
        'arguments':["--job.names=cdrCretJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    drotCdrJob_vol = []
    drotCdrJob_volMnt = []
    drotCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    drotCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    drotCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    drotCdrJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '675f5c588eb74e40acda04d01646912a',
        'volumes': drotCdrJob_vol,
        'volume_mounts': drotCdrJob_volMnt,
        'env_from':drotCdrJob_env,
        'task_id':'drotCdrJob',
        'image':'/icis/icis-rater-batch-cdr:20240729142001',
        'arguments':["--job.names=drotCdrJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('32a8058e29af4e9e990584cb701decc6')

    authCheck >> ldinCdrJob>>drotCdrJob>>cdrCretJob >> Complete
    








