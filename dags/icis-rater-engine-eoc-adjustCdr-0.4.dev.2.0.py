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
                , WORKFLOW_NAME='engine-eoc-adjustCdr',WORKFLOW_ID='51d53301b6174b14be7891d054d03583', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eoc-adjustCdr-0.4.dev.2.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 13, 57, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('51d53301b6174b14be7891d054d03583')

    adjustCdrJob_vol = []
    adjustCdrJob_volMnt = []
    adjustCdrJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    adjustCdrJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    adjustCdrJob_env = [getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret'), getICISConfigMap('icis-rater-engine-configmap'), getICISSecret('icis-rater-engine-secret')]
    adjustCdrJob_env.extend([getICISConfigMap('icis-rater-engine-eoc-batch-mng-configmap'), getICISSecret('icis-rater-engine-eoc-batch-mng-secret'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISSecret('icis-rater-engine-eoc-batch-secret')])
    adjustCdrJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    adjustCdrJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '996d72fb234745c98091589e7bd76f34',
        'volumes': adjustCdrJob_vol,
        'volume_mounts': adjustCdrJob_volMnt,
        'env_from':adjustCdrJob_env,
        'task_id':'adjustCdrJob',
        'image':'/icis/icis-rater-engine-eoc-batch:20250102180352',
        'arguments':["--job.names=adjustCdrJob","cyclYy=2024","cyclMonth=10","run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('51d53301b6174b14be7891d054d03583')

    workflow = COMMON.getICISPipeline([
        authCheck,
        adjustCdrJob,
        Complete
    ]) 

    # authCheck >> adjustCdrJob >> Complete
    workflow








