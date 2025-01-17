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
                , WORKFLOW_NAME='icis-rater-batch-cvintl-kt001',WORKFLOW_ID='22aa19e338f64e7d9031a337015a9900', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvintl-kt001-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 21, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('22aa19e338f64e7d9031a337015a9900')

    cvIntlKt001Job_vol = []
    cvIntlKt001Job_volMnt = []
    cvIntlKt001Job_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvIntlKt001Job_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvIntlKt001Job_env = [getICISConfigMap('icis-rater-batch-cvintl-configmap'), getICISConfigMap('icis-rater-batch-cvintl-configmap2'), getICISSecret('icis-rater-batch-cvintl-secret')]
    cvIntlKt001Job_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvIntlKt001Job_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvIntlKt001Job = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7204381ef32e4b4398766c345f3c8207',
        'volumes': cvIntlKt001Job_vol,
        'volume_mounts': cvIntlKt001Job_volMnt,
        'env_from':cvIntlKt001Job_env,
        'task_id':'cvIntlKt001Job',
        'image':'/icis/icis-rater-batch-cvintl:0.4.0.5',
        'arguments':["--job.names=cvIntlKt001Job", "cyclYm=202403"  ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('22aa19e338f64e7d9031a337015a9900')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvIntlKt001Job,
        Complete
    ]) 

    # authCheck >> cvIntlKt001Job >> Complete
    workflow








