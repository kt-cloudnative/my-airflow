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
                , WORKFLOW_NAME='icis-rater-batch-edi-calsumbill',WORKFLOW_ID='1d67bf53bbd94e01af6fc7e3f507d0bc', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-edi-calsumbill-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 14, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1d67bf53bbd94e01af6fc7e3f507d0bc')

    ediCalsumBillJob_vol = []
    ediCalsumBillJob_volMnt = []
    ediCalsumBillJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ediCalsumBillJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    ediCalsumBillJob_env = [getICISConfigMap('icis-rater-batch-edi-configmap'), getICISConfigMap('icis-rater-batch-edi-configmap2'), getICISSecret('icis-rater-batch-edi-secret')]
    ediCalsumBillJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ediCalsumBillJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ediCalsumBillJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ce337625e4974162af3c472033881b06',
        'volumes': ediCalsumBillJob_vol,
        'volume_mounts': ediCalsumBillJob_volMnt,
        'env_from':ediCalsumBillJob_env,
        'task_id':'ediCalsumBillJob',
        'image':'/icis/icis-rater-batch-edi:20240814171700',
        'arguments':["--job.names=ediCalsumBillJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1d67bf53bbd94e01af6fc7e3f507d0bc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ediCalsumBillJob,
        Complete
    ]) 

    # authCheck >> ediCalsumBillJob >> Complete
    workflow








