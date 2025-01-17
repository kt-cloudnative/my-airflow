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
                , WORKFLOW_NAME='icis-rater-batch-inchnairp',WORKFLOW_ID='c4a406e6780d4eab9a517adcda6c1e35', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-inchnairp-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 23, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c4a406e6780d4eab9a517adcda6c1e35')

    tlkChageSetlJob_vol = []
    tlkChageSetlJob_volMnt = []
    tlkChageSetlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    tlkChageSetlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    tlkChageSetlJob_env = [getICISConfigMap('icis-rater-batch-inchnairp-configmap'), getICISConfigMap('icis-rater-batch-inchnairp-configmap2'), getICISSecret('icis-rater-batch-inchnairp-secret')]
    tlkChageSetlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    tlkChageSetlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    tlkChageSetlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bd9e6a31fb714c43b2620811b545b86b',
        'volumes': tlkChageSetlJob_vol,
        'volume_mounts': tlkChageSetlJob_volMnt,
        'env_from':tlkChageSetlJob_env,
        'task_id':'tlkChageSetlJob',
        'image':'/icis/icis-rater-batch-inchnairp:0.4.0.2',
        'arguments':["--job.names=tlkChageSetlJob" , "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c4a406e6780d4eab9a517adcda6c1e35')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tlkChageSetlJob,
        Complete
    ]) 

    # authCheck >> tlkChageSetlJob >> Complete
    workflow








