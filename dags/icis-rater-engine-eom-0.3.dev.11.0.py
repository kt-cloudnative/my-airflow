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
                , WORKFLOW_NAME='engine-eom',WORKFLOW_ID='c64bc223e10047dca05d6a3cf51c028d', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-engine-eom-0.3.dev.11.0'
    ,'schedule_interval':'50 23 L * *'
    ,'start_date': datetime(2024, 2, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c64bc223e10047dca05d6a3cf51c028d')

    eomMainJob_vol = []
    eomMainJob_volMnt = []
    eomMainJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eomMainJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eomMainJob_env = [getICISConfigMap('icis-rater-engine-eom-batch-configmap'), getICISConfigMap('icis-rater-engine-eom-batch-configmap2'), getICISSecret('icis-rater-engine-eom-batch-secret')]
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eomMainJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3bba9ae394084e47ad8260a7090bf34e',
        'volumes': eomMainJob_vol,
        'volume_mounts': eomMainJob_volMnt,
        'env_from':eomMainJob_env,
        'task_id':'eomMainJob',
        'image':'/icis/icis-rater-engine-eom-batch:20241203153113',
        'arguments':["--job.names=eomMainJob", "cyclYy=${YYYY,DD,+1}", "cyclMonth=${MM,DD,+1}", "run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c64bc223e10047dca05d6a3cf51c028d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ///flow,
        eomMainJob,
        Complete
    ]) 

    # authCheck >> ///flow
eomMainJob >> Complete
    workflow








