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
                , WORKFLOW_NAME='uq-ncmbatch',WORKFLOW_ID='c7ea565317b84814b3e104c236f88dd9', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-uq-ncmbatch-0.1.dev.0.0'
    ,'schedule_interval':'* * * * *'
    ,'start_date': datetime(2024, 7, 23, 7, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c7ea565317b84814b3e104c236f88dd9')

    Task001_vol = []
    Task001_volMnt = []
    Task001_env = [getICISConfigMap('icis-rater-uq-ncmbatch-configmap'), getICISConfigMap('icis-rater-uq-ncmbatch-configmap2'), getICISSecret('icis-rater-uq-ncmbatch-secret')]
    Task001_env.extend([getICISConfigMap('icis-rater-uq-cmmn-configmap'), getICISSecret('icis-rater-uq-cmmn-secret')])
    Task001_env.extend([getICISConfigMap('icis-rater-uq-truststore.jks')])

    Task001 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a3fffb0e8a1144179a933b52c5990ec0',
        'volumes': Task001_vol,
        'volume_mounts': Task001_volMnt,
        'env_from':Task001_env,
        'task_id':'Task001',
        'image':'/icis/icis-rater-uq-ncmbatch:0.4.0.1',
        'arguments':["--job.names=ncmNowJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c7ea565317b84814b3e104c236f88dd9')

    authCheck >> Task001 >> Complete
    








