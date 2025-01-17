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
                , WORKFLOW_NAME='icis-rater-batch-voiprat',WORKFLOW_ID='b257be8354fd4756a623ca39d88958a4', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-voiprat-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 19, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b257be8354fd4756a623ca39d88958a4')

    missRoutJob_vol = []
    missRoutJob_volMnt = []
    missRoutJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    missRoutJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/batch_data/'))

    missRoutJob_env = [getICISConfigMap('icis-rater-batch-voiprat-configmap'), getICISConfigMap('icis-rater-batch-voiprat-configmap2'), getICISSecret('icis-rater-batch-voiprat-secret')]
    missRoutJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    missRoutJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    missRoutJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3a4bc1d5e7c649db8bfda7d83be5084d',
        'volumes': missRoutJob_vol,
        'volume_mounts': missRoutJob_volMnt,
        'env_from':missRoutJob_env,
        'task_id':'missRoutJob',
        'image':'/icis/icis-rater-batch-voiprat:20241209104242',
        'arguments':["--job.names=missRoutJob", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b257be8354fd4756a623ca39d88958a4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        missRoutJob,
        Complete
    ]) 

    # authCheck >> missRoutJob >> Complete
    workflow








