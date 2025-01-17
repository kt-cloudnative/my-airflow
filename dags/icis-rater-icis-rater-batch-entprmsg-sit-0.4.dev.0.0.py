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
                , WORKFLOW_NAME='icis-rater-batch-entprmsg-sit',WORKFLOW_ID='526999d1e8d8462989c6bea31a209fca', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-entprmsg-sit-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 23, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('526999d1e8d8462989c6bea31a209fca')

    netUseCostCmpJob_vol = []
    netUseCostCmpJob_volMnt = []
    netUseCostCmpJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    netUseCostCmpJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    netUseCostCmpJob_env = [getICISConfigMap('icis-rater-batch-entprmsg-configmap'), getICISConfigMap('icis-rater-batch-entprmsg-configmap2'), getICISSecret('icis-rater-batch-entprmsg-secret')]
    netUseCostCmpJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    netUseCostCmpJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    netUseCostCmpJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8c0c92229e0e475dbc5d2c5f46cf8dba',
        'volumes': netUseCostCmpJob_vol,
        'volume_mounts': netUseCostCmpJob_volMnt,
        'env_from':netUseCostCmpJob_env,
        'task_id':'netUseCostCmpJob',
        'image':'/icis/icis-rater-batch-entprmsg:0.4.0.1',
        'arguments':["--job.names=netUseCostCmpJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('526999d1e8d8462989c6bea31a209fca')

    workflow = COMMON.getICISPipeline([
        authCheck,
        netUseCostCmpJob,
        Complete
    ]) 

    # authCheck >> netUseCostCmpJob >> Complete
    workflow








