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
                , WORKFLOW_NAME='batchtest',WORKFLOW_ID='b2aceecce8c44fd3b841da26b95ca27c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-batchtest-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 6, 18, 5, 26, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b2aceecce8c44fd3b841da26b95ca27c')

    tkdgns_vol = []
    tkdgns_volMnt = []
    tkdgns_env = [getICISConfigMap('icis-rater-batchtest0619-configmap'), getICISConfigMap('icis-rater-batchtest0619-configmap2'), getICISSecret('icis-rater-batchtest0619-secret')]
    tkdgns_env.extend([getICISConfigMap('icis-rater-cmmn-configmap'), getICISSecret('icis-rater-cmmn-secret')])
    tkdgns_env.extend([getICISConfigMap('icis-rater-truststore.jks')])

    tkdgns = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7befa7429f844faaae715fd549fb2491',
        'volumes': tkdgns_vol,
        'volume_mounts': tkdgns_volMnt,
        'env_from':tkdgns_env,
        'task_id':'tkdgns',
        'image':'/icis/icis-rater-batchtest0619:20240619200936',
        'arguments':["--job.names=tkdgns"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b2aceecce8c44fd3b841da26b95ca27c')

    authCheck >> tkdgns >> Complete
    








