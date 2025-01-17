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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-cvWrlinJob',WORKFLOW_ID='ddbc1a0df992471f881d6ed49b7a0713', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-cvWrlinJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 30, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ddbc1a0df992471f881d6ed49b7a0713')

    cvWrlinJob_vol = []
    cvWrlinJob_volMnt = []
    cvWrlinJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvWrlinJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvWrlinJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvWrlinJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvWrlinJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '16baf362267b4176a44d4a8998551c85',
        'volumes': cvWrlinJob_vol,
        'volume_mounts': cvWrlinJob_volMnt,
        'env_from':cvWrlinJob_env,
        'task_id':'cvWrlinJob',
        'image':'/icis/icis-rater-batch-cv1541:0.4.0.15',
        'arguments':["--job.names=cvWrlinJob",  "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ddbc1a0df992471f881d6ed49b7a0713')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvWrlinJob,
        Complete
    ]) 

    # authCheck >> cvWrlinJob >> Complete
    workflow








