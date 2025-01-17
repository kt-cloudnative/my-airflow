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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-wlessBizr',WORKFLOW_ID='91cd684e01af485ea609bf030a77bb27', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-wlessBizr-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 4, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('91cd684e01af485ea609bf030a77bb27')

    wless1541CvTgtRegJob_vol = []
    wless1541CvTgtRegJob_volMnt = []
    wless1541CvTgtRegJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    wless1541CvTgtRegJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    wless1541CvTgtRegJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    wless1541CvTgtRegJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    wless1541CvTgtRegJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    wless1541CvTgtRegJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3b0bffe6a5a143b29dd70d658cfd8cc3',
        'volumes': wless1541CvTgtRegJob_vol,
        'volume_mounts': wless1541CvTgtRegJob_volMnt,
        'env_from':wless1541CvTgtRegJob_env,
        'task_id':'wless1541CvTgtRegJob',
        'image':'/icis/icis-rater-batch-cv1541:20240905144237',
        'arguments':["--job.names=wless1541CvTgtRegJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('91cd684e01af485ea609bf030a77bb27')

    workflow = COMMON.getICISPipeline([
        authCheck,
        wless1541CvTgtRegJob,
        Complete
    ]) 

    # authCheck >> wless1541CvTgtRegJob >> Complete
    workflow








