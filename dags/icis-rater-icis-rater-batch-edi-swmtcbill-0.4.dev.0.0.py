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
                , WORKFLOW_NAME='icis-rater-batch-edi-swmtcbill',WORKFLOW_ID='18270c558e164b8cbd44017050d17077', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-edi-swmtcbill-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 14, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('18270c558e164b8cbd44017050d17077')

    ediSwMtcBillJob_vol = []
    ediSwMtcBillJob_volMnt = []
    ediSwMtcBillJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    ediSwMtcBillJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    ediSwMtcBillJob_env = [getICISConfigMap('icis-rater-batch-edi-configmap'), getICISConfigMap('icis-rater-batch-edi-configmap2'), getICISSecret('icis-rater-batch-edi-secret')]
    ediSwMtcBillJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    ediSwMtcBillJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    ediSwMtcBillJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f6fa45dde95848bab42b7b3e07753fc3',
        'volumes': ediSwMtcBillJob_vol,
        'volume_mounts': ediSwMtcBillJob_volMnt,
        'env_from':ediSwMtcBillJob_env,
        'task_id':'ediSwMtcBillJob',
        'image':'/icis/icis-rater-batch-edi:20240814171700',
        'arguments':["--job.names=ediSwMtcBillJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('18270c558e164b8cbd44017050d17077')

    workflow = COMMON.getICISPipeline([
        authCheck,
        ediSwMtcBillJob,
        Complete
    ]) 

    # authCheck >> ediSwMtcBillJob >> Complete
    workflow








