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
                , WORKFLOW_NAME='icis-rater-batch-tung',WORKFLOW_ID='2e07ddecfeb1474db2d56d1b6c6aa75a', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-tung-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 8, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2e07ddecfeb1474db2d56d1b6c6aa75a')

    tungDcSoipJob_vol = []
    tungDcSoipJob_volMnt = []
    tungDcSoipJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    tungDcSoipJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    tungDcSoipJob_env = [getICISConfigMap('icis-rater-batch-tung-configmap'), getICISConfigMap('icis-rater-batch-tung-configmap2'), getICISSecret('icis-rater-batch-tung-secret')]
    tungDcSoipJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    tungDcSoipJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    tungDcSoipJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8fc5d6f2e5534758b29754e13baeac07',
        'volumes': tungDcSoipJob_vol,
        'volume_mounts': tungDcSoipJob_volMnt,
        'env_from':tungDcSoipJob_env,
        'task_id':'tungDcSoipJob',
        'image':'/icis/icis-rater-batch-tung:20240909154608',
        'arguments':["--job.names=tungDcSoipJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2e07ddecfeb1474db2d56d1b6c6aa75a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tungDcSoipJob,
        Complete
    ]) 

    # authCheck >> tungDcSoipJob >> Complete
    workflow








