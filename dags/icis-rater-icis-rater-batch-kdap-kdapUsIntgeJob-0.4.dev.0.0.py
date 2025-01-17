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
                , WORKFLOW_NAME='icis-rater-batch-kdap-kdapUsIntgeJob',WORKFLOW_ID='8f1e298e05464c09b4557660b49ae8ab', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-kdap-kdapUsIntgeJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8f1e298e05464c09b4557660b49ae8ab')

    kdapUsIntgeJob_vol = []
    kdapUsIntgeJob_volMnt = []
    kdapUsIntgeJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kdapUsIntgeJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kdapUsIntgeJob_env = [getICISConfigMap('icis-rater-batch-kdap-configmap'), getICISConfigMap('icis-rater-batch-kdap-configmap2'), getICISSecret('icis-rater-batch-kdap-secret')]
    kdapUsIntgeJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kdapUsIntgeJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kdapUsIntgeJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '27298900a1de46d8a5e1daee157f65eb',
        'volumes': kdapUsIntgeJob_vol,
        'volume_mounts': kdapUsIntgeJob_volMnt,
        'env_from':kdapUsIntgeJob_env,
        'task_id':'kdapUsIntgeJob',
        'image':'/icis/icis-rater-batch-kdap:0.4.0.37',
        'arguments':["--job.names=kdapUsIntgeJob", "retvType=M", "cyclYm=202407", "cyclGroup=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8f1e298e05464c09b4557660b49ae8ab')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kdapUsIntgeJob,
        Complete
    ]) 

    # authCheck >> kdapUsIntgeJob >> Complete
    workflow








