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
                , WORKFLOW_NAME='entprmsg_dev_240729',WORKFLOW_ID='6d5b3eea84d74ce086d0c0ddbba77d90', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-entprmsg_dev_240729-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 29, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6d5b3eea84d74ce086d0c0ddbba77d90')

    netUseCostCmpJob_vol = []
    netUseCostCmpJob_volMnt = []
    netUseCostCmpJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    netUseCostCmpJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    netUseCostCmpJob_env = [getICISConfigMap('icis-rater-batch-entprmsg-configmap'), getICISConfigMap('icis-rater-batch-entprmsg-configmap2'), getICISSecret('icis-rater-batch-entprmsg-secret')]
    netUseCostCmpJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    netUseCostCmpJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    netUseCostCmpJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '22fbbe6d66674ee0824ab2c6664980bf',
        'volumes': netUseCostCmpJob_vol,
        'volume_mounts': netUseCostCmpJob_volMnt,
        'env_from':netUseCostCmpJob_env,
        'task_id':'netUseCostCmpJob',
        'image':'/icis/icis-rater-batch-entprmsg:20240729131510',
        'arguments':["--job.names=netUseCostCmpJob","runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6d5b3eea84d74ce086d0c0ddbba77d90')

    authCheck >> netUseCostCmpJob >> Complete
    








