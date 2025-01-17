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
                , WORKFLOW_NAME='icis-rater-batch-voiprat-soipKtIntelnetJob',WORKFLOW_ID='2ce0813d5e684942a9a9fc5117546f5a', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-voiprat-soipKtIntelnetJob-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2024, 12, 30, 10, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2ce0813d5e684942a9a9fc5117546f5a')

    soipKtIntelnetJob_vol = []
    soipKtIntelnetJob_volMnt = []
    soipKtIntelnetJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    soipKtIntelnetJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    soipKtIntelnetJob_env = [getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret'), getICISConfigMap('icis-rater-batch-configmap'), getICISSecret('icis-rater-batch-secret')]
    soipKtIntelnetJob_env.extend([getICISConfigMap('icis-rater-batch-voiprat-mng-configmap'), getICISSecret('icis-rater-batch-voiprat-mng-secret'), getICISConfigMap('icis-rater-batch-voiprat-configmap'), getICISSecret('icis-rater-batch-voiprat-secret')])
    soipKtIntelnetJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    soipKtIntelnetJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '484919106d7f444485c72b91b671a679',
        'volumes': soipKtIntelnetJob_vol,
        'volume_mounts': soipKtIntelnetJob_volMnt,
        'env_from':soipKtIntelnetJob_env,
        'task_id':'soipKtIntelnetJob',
        'image':'/icis/icis-rater-batch-voiprat:0.4.0.12',
        'arguments':["--job.names=soipKtIntelnetJob" , "wrkYmd=20240701", "jobDivVal=C"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2ce0813d5e684942a9a9fc5117546f5a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        soipKtIntelnetJob,
        Complete
    ]) 

    # authCheck >> soipKtIntelnetJob >> Complete
    workflow








