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
                , WORKFLOW_NAME='npfa-forec-data',WORKFLOW_ID='dfeeb5dcaa5d4e3fae700b4f09e07698', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-npfa-forec-data-0.1.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('dfeeb5dcaa5d4e3fae700b4f09e07698')

    task001_vol = []
    task001_volMnt = []
    task001_env = [getICISConfigMap('icis-rater-uq-npfa-forec-data-configmap'), getICISConfigMap('icis-rater-uq-npfa-forec-data-configmap2'), getICISSecret('icis-rater-uq-npfa-forec-data-secret')]
    task001_env.extend([getICISConfigMap('icis-rater-uq-cmmn-configmap'), getICISSecret('icis-rater-uq-cmmn-secret')])
    task001_env.extend([getICISConfigMap('icis-rater-uq-truststore.jks')])

    task001 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c2c93aeb1e964c668ab0e240586070fb',
        'volumes': task001_vol,
        'volume_mounts': task001_volMnt,
        'env_from':task001_env,
        'task_id':'task001',
        'image':'/icis/icis-rater-uq-npfa-forec-data:0.4.0.1',
        'arguments':["--job.names=npfaforecJob", "billYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('dfeeb5dcaa5d4e3fae700b4f09e07698')

    workflow = COMMON.getICISPipeline([
        authCheck,
        task001,
        Complete
    ]) 

    # authCheck >> task001 >> Complete
    workflow








