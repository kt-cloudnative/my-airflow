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
                , WORKFLOW_NAME='callforwading-dev',WORKFLOW_ID='5465e05672104e69b3e7917c1d326a94', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-callforwading-dev-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 23, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5465e05672104e69b3e7917c1d326a94')

    callForwardingJob_vol = []
    callForwardingJob_volMnt = []
    callForwardingJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    callForwardingJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    callForwardingJob_env = [getICISConfigMap('icis-rater-batch-callforwarding-configmap'), getICISConfigMap('icis-rater-batch-callforwarding-configmap2'), getICISSecret('icis-rater-batch-callforwarding-secret')]
    callForwardingJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    callForwardingJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    callForwardingJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '49b3b846051f43cab6db043da3619a9e',
        'volumes': callForwardingJob_vol,
        'volume_mounts': callForwardingJob_volMnt,
        'env_from':callForwardingJob_env,
        'task_id':'callForwardingJob',
        'image':'/icis/icis-rater-batch-callforwarding:0.4.0.1',
        'arguments':["--job.names=callForwardingJob", "runType=T", "useYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5465e05672104e69b3e7917c1d326a94')

    workflow = COMMON.getICISPipeline([
        authCheck,
        callForwardingJob,
        Complete
    ]) 

    # authCheck >> callForwardingJob >> Complete
    workflow








