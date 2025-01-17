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
                , WORKFLOW_NAME='icis-rater-batch-inmar-040',WORKFLOW_ID='ceb4f213128c4182a816b78097d00fb8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-inmar-040-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 19, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ceb4f213128c4182a816b78097d00fb8')

    inmarTgtCretJob_vol = []
    inmarTgtCretJob_volMnt = []
    inmarTgtCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    inmarTgtCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    inmarTgtCretJob_env = [getICISConfigMap('icis-rater-batch-inmar-configmap'), getICISConfigMap('icis-rater-batch-inmar-configmap2'), getICISSecret('icis-rater-batch-inmar-secret')]
    inmarTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    inmarTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    inmarTgtCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c789a0983c7b4915b77bb06c7c1aba7e',
        'volumes': inmarTgtCretJob_vol,
        'volume_mounts': inmarTgtCretJob_volMnt,
        'env_from':inmarTgtCretJob_env,
        'task_id':'inmarTgtCretJob',
        'image':'/icis/icis-rater-batch-inmar:20240819142418',
        'arguments':["--job.names=inmarTgtCretJob", "cyclYmd=${YYYYMMDD}" , "cyclYm=${YYYYMMDD, DD, -01}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ceb4f213128c4182a816b78097d00fb8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        inmarTgtCretJob,
        Complete
    ]) 

    # authCheck >> inmarTgtCretJob >> Complete
    workflow








