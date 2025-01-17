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
                , WORKFLOW_NAME='icis-rater-batch-intltel-intlInvoice',WORKFLOW_ID='e50397f003d941959b479a704fdadea9', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intltel-intlInvoice-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e50397f003d941959b479a704fdadea9')

    invoiceJob_vol = []
    invoiceJob_volMnt = []
    invoiceJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    invoiceJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    invoiceJob_env = [getICISConfigMap('icis-rater-batch-intltel-configmap'), getICISConfigMap('icis-rater-batch-intltel-configmap2'), getICISSecret('icis-rater-batch-intltel-secret')]
    invoiceJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    invoiceJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    invoiceJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5f1db48d25cb413799839da8d1f7b7c3',
        'volumes': invoiceJob_vol,
        'volume_mounts': invoiceJob_volMnt,
        'env_from':invoiceJob_env,
        'task_id':'invoiceJob',
        'image':'/icis/icis-rater-batch-intltel:20240903095318',
        'arguments':["--job.names=invoiceJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e50397f003d941959b479a704fdadea9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        invoiceJob,
        Complete
    ]) 

    # authCheck >> invoiceJob >> Complete
    workflow








