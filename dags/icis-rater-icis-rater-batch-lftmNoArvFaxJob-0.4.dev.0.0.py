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
                , WORKFLOW_NAME='icis-rater-batch-lftmNoArvFaxJob',WORKFLOW_ID='ca26cdefc06b44eebba7c5b8acb94702', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-lftmNoArvFaxJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ca26cdefc06b44eebba7c5b8acb94702')

    lftmNoArvFaxJob_vol = []
    lftmNoArvFaxJob_volMnt = []
    lftmNoArvFaxJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lftmNoArvFaxJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lftmNoArvFaxJob_env = [getICISConfigMap('icis-rater-batch-billif-configmap'), getICISConfigMap('icis-rater-batch-billif-configmap2'), getICISSecret('icis-rater-batch-billif-secret')]
    lftmNoArvFaxJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lftmNoArvFaxJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lftmNoArvFaxJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '65f5a92b8f0341548952955645b77828',
        'volumes': lftmNoArvFaxJob_vol,
        'volume_mounts': lftmNoArvFaxJob_volMnt,
        'env_from':lftmNoArvFaxJob_env,
        'task_id':'lftmNoArvFaxJob',
        'image':'/icis/icis-rater-batch-billif:0.4.0.22',
        'arguments':["--job.names=lftmNoArvFaxJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ca26cdefc06b44eebba7c5b8acb94702')

    workflow = COMMON.getICISPipeline([
        authCheck,
        lftmNoArvFaxJob,
        Complete
    ]) 

    # authCheck >> lftmNoArvFaxJob >> Complete
    workflow








