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
                , WORKFLOW_NAME='eoc_work1',WORKFLOW_ID='b1d23db558f04db9a2e059abf950786b', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-eoc_work1-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 24, 10, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b1d23db558f04db9a2e059abf950786b')

    eocMainJob_vol = []
    eocMainJob_volMnt = []
    eocMainJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eocMainJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eocMainJob_env = [getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap2'), getICISSecret('icis-rater-engine-eoc-batch-secret')]
    eocMainJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eocMainJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eocMainJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0770f1a5822d4ae5a267952903431cf9',
        'volumes': eocMainJob_vol,
        'volume_mounts': eocMainJob_volMnt,
        'env_from':eocMainJob_env,
        'task_id':'eocMainJob',
        'image':'/icis/icis-rater-engine-eoc-batch:20240729164450',
        'arguments':["--job.names=eocMainJob", "cyclYy=${YYYY,DD,-1}", "cyclMonth=${MM,DD,-1}", "run.id=${YYYYMMDDHHMISS}", "--HIKARI-POOL-SIZE=10"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b1d23db558f04db9a2e059abf950786b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        eocMainJob,
        Complete
    ]) 

    # authCheck >> eocMainJob >> Complete
    workflow








