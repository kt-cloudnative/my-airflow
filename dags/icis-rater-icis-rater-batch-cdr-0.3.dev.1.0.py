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
    'dag_id':'icis-rater-icis-rater-batch-cdr-0.3.dev.1.0',
    'schedule_interval':'@once',
    'start_date': datetime(2024, 1, 12, 0, 59, 00, tzinfo=local_tz),
    'end_date': None,
    'paused': True
})as dag:

    authCheck = getICISAuthCheckWflow('36fc82acf9c94da7b6df7340fb313695')

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
        'id' : 'f75fa75c578f47179d81ac682a626aed',
        'volumes': cdrCretJob_vol,
        'volume_mounts': cdrCretJob_volMnt,
        'env_from':cdrCretJob_env,
        'task_id':'cdrCretJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:0.3.0.3',
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
        'id' : '4fff8d4dcf6d4270b625490529de6dff',
        'volumes': drotCdrJob_vol,
        'volume_mounts': drotCdrJob_volMnt,
        'env_from':drotCdrJob_env,
        'task_id':'drotCdrJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:0.3.0.3',
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
        'id' : 'a4daecf9aeb0422b99976915a80a4f8c',
        'volumes': ldinCdrJob_vol,
        'volume_mounts': ldinCdrJob_volMnt,
        'env_from':ldinCdrJob_env,
        'task_id':'ldinCdrJob',
        'image':'nexus.dspace.kt.co.kr/icis/icis-rater-batch-cdr:0.3.0.3',
        'arguments':["--job.names=ldinCdrJob", "runType=T", "useYm=202311"]
    })


    Complete = getICISCompleteWflowTask('36fc82acf9c94da7b6df7340fb313695')

    authCheck >> ldinCdrJob>>drotCdrJob>>cdrCretJob >> Complete
    








