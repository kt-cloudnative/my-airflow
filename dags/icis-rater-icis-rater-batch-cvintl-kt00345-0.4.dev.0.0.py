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
                , WORKFLOW_NAME='icis-rater-batch-cvintl-kt00345',WORKFLOW_ID='6671e940e61848329b11f1b771f98f98', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvintl-kt00345-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 21, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6671e940e61848329b11f1b771f98f98')

    cvIntlKt00345Job_vol = []
    cvIntlKt00345Job_volMnt = []
    cvIntlKt00345Job_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvIntlKt00345Job_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvIntlKt00345Job_env = [getICISConfigMap('icis-rater-batch-cvintl-configmap'), getICISConfigMap('icis-rater-batch-cvintl-configmap2'), getICISSecret('icis-rater-batch-cvintl-secret')]
    cvIntlKt00345Job_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvIntlKt00345Job_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvIntlKt00345Job = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'aad74dc918074068a403ae7519b1f34d',
        'volumes': cvIntlKt00345Job_vol,
        'volume_mounts': cvIntlKt00345Job_volMnt,
        'env_from':cvIntlKt00345Job_env,
        'task_id':'cvIntlKt00345Job',
        'image':'/icis/icis-rater-batch-cvintl:0.4.0.5',
        'arguments':["--job.names=cvIntlKt00345Job", "cyclYm=202403" ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6671e940e61848329b11f1b771f98f98')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvIntlKt00345Job,
        Complete
    ]) 

    # authCheck >> cvIntlKt00345Job >> Complete
    workflow








