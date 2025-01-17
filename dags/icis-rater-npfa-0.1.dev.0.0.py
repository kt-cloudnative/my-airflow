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
                , WORKFLOW_NAME='npfa',WORKFLOW_ID='027b6668f70f435dabbea3b27856e01c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-npfa-0.1.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 9, 5, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('027b6668f70f435dabbea3b27856e01c')

    task123_vol = []
    task123_volMnt = []
    task123_env = [getICISConfigMap('icis-rater-uq-npfa-data-configmap'), getICISConfigMap('icis-rater-uq-npfa-data-configmap2'), getICISSecret('icis-rater-uq-npfa-data-secret')]
    task123_env.extend([getICISConfigMap('icis-rater-uq-cmmn-configmap'), getICISSecret('icis-rater-uq-cmmn-secret')])
    task123_env.extend([getICISConfigMap('icis-rater-uq-truststore.jks')])

    task123 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '00b33f50aed14c70a1c0f2b147d51a24',
        'volumes': task123_vol,
        'volume_mounts': task123_volMnt,
        'env_from':task123_env,
        'task_id':'task123',
        'image':'/icis/icis-rater-uq-npfa-data:0.4.0.1',
        'arguments':["--job.names=npfaforecJob", "billYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('027b6668f70f435dabbea3b27856e01c')

    authCheck >> task123 >> Complete
    








