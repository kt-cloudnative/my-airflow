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
                , WORKFLOW_NAME='icis-rater-batch-cv1541-cvWlessMm-sit',WORKFLOW_ID='85a602098a5c4aa19e00e951bb3b28e3', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv1541-cvWlessMm-sit-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('85a602098a5c4aa19e00e951bb3b28e3')

    cvWlessMmJob_vol = []
    cvWlessMmJob_volMnt = []
    cvWlessMmJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvWlessMmJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvWlessMmJob_env = [getICISConfigMap('icis-rater-batch-cv1541-configmap'), getICISConfigMap('icis-rater-batch-cv1541-configmap2'), getICISSecret('icis-rater-batch-cv1541-secret')]
    cvWlessMmJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvWlessMmJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvWlessMmJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ccc49e32cda749d395f9259696979193',
        'volumes': cvWlessMmJob_vol,
        'volume_mounts': cvWlessMmJob_volMnt,
        'env_from':cvWlessMmJob_env,
        'task_id':'cvWlessMmJob',
        'image':'/icis/icis-rater-batch-cv1541:0.4.0.14',
        'arguments':["--job.names=cvWlessMmJob" , "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('85a602098a5c4aa19e00e951bb3b28e3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvWlessMmJob,
        Complete
    ]) 

    # authCheck >> cvWlessMmJob >> Complete
    workflow








