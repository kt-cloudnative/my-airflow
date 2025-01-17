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
                , WORKFLOW_NAME='icis-rater-batch-orrgtblcurn',WORKFLOW_ID='f7b9b0a36ccb474a890b1c5a6f0de281', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-orrgtblcurn-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 10, 6, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f7b9b0a36ccb474a890b1c5a6f0de281')

    orrgTblCurnJob_vol = []
    orrgTblCurnJob_volMnt = []
    orrgTblCurnJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    orrgTblCurnJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    orrgTblCurnJob_env = [getICISConfigMap('icis-rater-batch-orrgtblcurn-configmap'), getICISConfigMap('icis-rater-batch-orrgtblcurn-configmap2'), getICISSecret('icis-rater-batch-orrgtblcurn-secret')]
    orrgTblCurnJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    orrgTblCurnJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    orrgTblCurnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3a4aa3607f614de08cbf951b1be71adf',
        'volumes': orrgTblCurnJob_vol,
        'volume_mounts': orrgTblCurnJob_volMnt,
        'env_from':orrgTblCurnJob_env,
        'task_id':'orrgTblCurnJob',
        'image':'/icis/icis-rater-batch-orrgtblcurn:0.4.0.1',
        'arguments':["--job.names=orrgTblCurnJob", "jobDate=${YYYYMMDD}", "jobDay=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f7b9b0a36ccb474a890b1c5a6f0de281')

    workflow = COMMON.getICISPipeline([
        authCheck,
        orrgTblCurnJob,
        Complete
    ]) 

    # authCheck >> orrgTblCurnJob >> Complete
    workflow








