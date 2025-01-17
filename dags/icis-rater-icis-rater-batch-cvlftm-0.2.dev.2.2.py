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
                , WORKFLOW_NAME='icis-rater-batch-cvlftm',WORKFLOW_ID='b8a2e0f4d37a4e86b1fcfd3c6eb8c112', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvlftm-0.2.dev.2.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b8a2e0f4d37a4e86b1fcfd3c6eb8c112')

    cvLftmNoJob_vol = []
    cvLftmNoJob_volMnt = []
    cvLftmNoJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvLftmNoJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvLftmNoJob_env = [getICISConfigMap('icis-rater-batch-cvlftm-configmap'), getICISConfigMap('icis-rater-batch-cvlftm-configmap2'), getICISSecret('icis-rater-batch-cvlftm-secret')]
    cvLftmNoJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvLftmNoJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvLftmNoJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b820d66a501f4e429c6d03f835e65c65',
        'volumes': cvLftmNoJob_vol,
        'volume_mounts': cvLftmNoJob_volMnt,
        'env_from':cvLftmNoJob_env,
        'task_id':'cvLftmNoJob',
        'image':'/icis/icis-rater-batch-cvlftm:20240329154813',
        'arguments':["--job.names=cvLftmNoJob", "runType=T", "cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b8a2e0f4d37a4e86b1fcfd3c6eb8c112')

    authCheck >> cvLftmNoJob >> Complete
    








