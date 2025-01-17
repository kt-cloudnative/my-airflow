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
                , WORKFLOW_NAME='guiding-bulk-load',WORKFLOW_ID='fea0f710e25f4a3aa948170c1174cab1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-guiding-bulk-load-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 17, 16, 57, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fea0f710e25f4a3aa948170c1174cab1')


      
       
    guidingBulkLoad = COMMON.getAgentHttpOperator({
        'id': '1eac2ca742414ced920748d3917ebb70',
        'task_id': 'guidingBulkLoad',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.0.0",
  "wflowId" : "fea0f710e25f4a3aa948170c1174cab1",
  "wflowNm" : "guiding-bulk-load",
  "wflowTaskId" : "1eac2ca742414ced920748d3917ebb70",
  "taskId" : "guidingBulkLoad",
  "cretDt" : "2025-01-17T07:57:41.145128Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-engine-guiding-loader-server/guiding-loader/redissvcbulkload",
  "jobData" : {}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('fea0f710e25f4a3aa948170c1174cab1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        guidingBulkLoad,
        Complete
    ]) 

    # authCheck >> guidingBulkLoad >> Complete
    workflow








