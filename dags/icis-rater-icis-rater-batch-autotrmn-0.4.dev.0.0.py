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
                , WORKFLOW_NAME='icis-rater-batch-autotrmn',WORKFLOW_ID='cc96b800c2e7409592debd1516d78aae', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-autotrmn-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 27, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cc96b800c2e7409592debd1516d78aae')

    intlTelPauseCustCretJob_vol = []
    intlTelPauseCustCretJob_volMnt = []
    intlTelPauseCustCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intlTelPauseCustCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intlTelPauseCustCretJob_env = [getICISConfigMap('icis-rater-batch-autotrmn-configmap'), getICISConfigMap('icis-rater-batch-autotrmn-configmap2'), getICISSecret('icis-rater-batch-autotrmn-secret')]
    intlTelPauseCustCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intlTelPauseCustCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intlTelPauseCustCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b543c202f92d445fa375a9ddb8e49ffd',
        'volumes': intlTelPauseCustCretJob_vol,
        'volume_mounts': intlTelPauseCustCretJob_volMnt,
        'env_from':intlTelPauseCustCretJob_env,
        'task_id':'intlTelPauseCustCretJob',
        'image':'/icis/icis-rater-batch-autotrmn:0.4.0.1',
        'arguments':["--job.names=intlTelPauseCustCretJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cc96b800c2e7409592debd1516d78aae')

    workflow = COMMON.getICISPipeline([
        authCheck,
        intlTelPauseCustCretJob,
        Complete
    ]) 

    # authCheck >> intlTelPauseCustCretJob >> Complete
    workflow








