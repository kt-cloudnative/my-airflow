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
                , WORKFLOW_NAME='rds-test',WORKFLOW_ID='92cf58b301c64c6e8497aaf04772ec85', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-rds-test-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 30, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('92cf58b301c64c6e8497aaf04772ec85')

    rdsSetlJob_vol = []
    rdsSetlJob_volMnt = []
    rdsSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    rdsSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    rdsSetlJob_env = [getICISConfigMap('icis-rater-batch-rds-configmap'), getICISConfigMap('icis-rater-batch-rds-configmap2'), getICISSecret('icis-rater-batch-rds-secret')]
    rdsSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    rdsSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    rdsSetlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a07e796e98b64447bcb0a9e17e9830d4',
        'volumes': rdsSetlJob_vol,
        'volume_mounts': rdsSetlJob_volMnt,
        'env_from':rdsSetlJob_env,
        'task_id':'rdsSetlJob',
        'image':'/icis/icis-rater-batch-rds:0.4.0.1',
        'arguments':["--job.names=rdsSetlJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('92cf58b301c64c6e8497aaf04772ec85')

    workflow = COMMON.getICISPipeline([
        authCheck,
        rdsSetlJob,
        Complete
    ]) 

    # authCheck >> rdsSetlJob >> Complete
    workflow








