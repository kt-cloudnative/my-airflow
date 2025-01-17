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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg-lgu',WORKFLOW_ID='3c28e44d441548c1958ebc202b1fde3c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg-lgu-0.4.dev.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 21, 3, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3c28e44d441548c1958ebc202b1fde3c')

    lgBfacSelJob_vol = []
    lgBfacSelJob_volMnt = []
    lgBfacSelJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lgBfacSelJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lgBfacSelJob_env = [getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISConfigMap('icis-rater-batch-otcorrg-configmap2'), getICISSecret('icis-rater-batch-otcorrg-secret')]
    lgBfacSelJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lgBfacSelJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lgBfacSelJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '54ab19e14e5346079ac8eca5c9bef79c',
        'volumes': lgBfacSelJob_vol,
        'volume_mounts': lgBfacSelJob_volMnt,
        'env_from':lgBfacSelJob_env,
        'task_id':'lgBfacSelJob',
        'image':'/icis/icis-rater-batch-otcorrg:20240910175748',
        'arguments':["--job.names=lgBfacSelJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3c28e44d441548c1958ebc202b1fde3c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        lgBfacSelJob,
        Complete
    ]) 

    # authCheck >> lgBfacSelJob >> Complete
    workflow








