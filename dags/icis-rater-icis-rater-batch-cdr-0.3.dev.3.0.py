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

from icis_common_staging import *
COMMON = ICISCmmn(DOMAIN='rater',ENV='dev', NAMESPACE='t-rater')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cdr-0.3.dev.3.0',
    'schedule_interval':'@once',
    'start_date': datetime(2024, 2, 5, 0, 59, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('8fb008522df44e9588f024290178183a')

    cdrCretJob_vol = []
    cdrCretJob_volMnt = []
    cdrCretJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data/'))

    cdrCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cdrCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cdrCretJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cdrCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cdrCretJob = COMMON.getICISKubernetesPodOperator({
        'id' : '3e994ff82c2a4899bb29281f012e2e06',
        'volumes': cdrCretJob_vol,
        'volume_mounts': cdrCretJob_volMnt,
        'env_from':cdrCretJob_env,
        'task_id':'cdrCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:20240226133802',
        'arguments':["--job.names=cdrCretJob", "runType=T", "useYm=202311"]
    })


    drotCdrJob_vol = []
    drotCdrJob_volMnt = []
    drotCdrJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    drotCdrJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data/'))

    drotCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    drotCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    drotCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    drotCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    drotCdrJob = COMMON.getICISKubernetesPodOperator({
        'id' : '7d01c0f801214cbe982567e1b307ebc5',
        'volumes': drotCdrJob_vol,
        'volume_mounts': drotCdrJob_volMnt,
        'env_from':drotCdrJob_env,
        'task_id':'drotCdrJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:20240226133802',
        'arguments':["--job.names=drotCdrJob", "runType=T", "useYm=202311"]
    })


    ldinCdrJob_vol = []
    ldinCdrJob_volMnt = []
    ldinCdrJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    ldinCdrJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data/'))

    ldinCdrJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ldinCdrJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    ldinCdrJob_env = [getICISConfigMap('icis-rater-batch-cdr-configmap'), getICISConfigMap('icis-rater-batch-cdr-configmap2'), getICISSecret('icis-rater-batch-cdr-secret')]
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ldinCdrJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ldinCdrJob = COMMON.getICISKubernetesPodOperator({
        'id' : '1dac8bf99d6943c59dcdfea272056134',
        'volumes': ldinCdrJob_vol,
        'volume_mounts': ldinCdrJob_volMnt,
        'env_from':ldinCdrJob_env,
        'task_id':'ldinCdrJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:20240226133802',
        'arguments':["--job.names=ldinCdrJob", "runType=T", "useYm=202311"]
    })


    Complete = getICISCompleteWflowTask('8fb008522df44e9588f024290178183a')

    authCheck >> ldinCdrJob>>drotCdrJob>>cdrCretJob >> Complete
    








