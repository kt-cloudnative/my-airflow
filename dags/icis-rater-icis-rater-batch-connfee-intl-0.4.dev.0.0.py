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
                , WORKFLOW_NAME='icis-rater-batch-connfee-intl',WORKFLOW_ID='a3a363ffe950424f94460184fa6022b8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-connfee-intl-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 23, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a3a363ffe950424f94460184fa6022b8')

    connfeeSetlIntlJob_vol = []
    connfeeSetlIntlJob_volMnt = []
    connfeeSetlIntlJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    connfeeSetlIntlJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    connfeeSetlIntlJob_env = [getICISConfigMap('icis-rater-batch-connfee-configmap'), getICISConfigMap('icis-rater-batch-connfee-configmap2'), getICISSecret('icis-rater-batch-connfee-secret')]
    connfeeSetlIntlJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    connfeeSetlIntlJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    connfeeSetlIntlJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'eada294237094d40b3ca94e0b89b4082',
        'volumes': connfeeSetlIntlJob_vol,
        'volume_mounts': connfeeSetlIntlJob_volMnt,
        'env_from':connfeeSetlIntlJob_env,
        'task_id':'connfeeSetlIntlJob',
        'image':'/icis/icis-rater-batch-connfee:20240821173253',
        'arguments':["--job.names=connfeeSetlIntlJob", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a3a363ffe950424f94460184fa6022b8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        connfeeSetlIntlJob,
        Complete
    ]) 

    # authCheck >> connfeeSetlIntlJob >> Complete
    workflow








