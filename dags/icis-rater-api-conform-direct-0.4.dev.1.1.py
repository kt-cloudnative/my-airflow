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
                , WORKFLOW_NAME='api-conform-direct',WORKFLOW_ID='63eded4723be40ea9852610a72b09655', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api-conform-direct-0.4.dev.1.1'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 20, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('63eded4723be40ea9852610a72b09655')


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': '958b6bdb197d48c7b3970c8f35e604cc',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.1",
  "wflowId" : "63eded4723be40ea9852610a72b09655",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "958b6bdb197d48c7b3970c8f35e604cc",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-12-06T05:37:31.723741Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-engine-hotbill-server/hotbill/tibero",
  "jobData" : {    "inPayLoad": {        "err": "true"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': '96e01e9a8afd4f5483143e40228d3d3d',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.1",
  "wflowId" : "63eded4723be40ea9852610a72b09655",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "96e01e9a8afd4f5483143e40228d3d3d",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-12-06T05:37:31.736301Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/tibero",
  "jobData" : {}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': '3c4f59c6b88846a9a6db8afef7ed8a7f',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.1",
  "wflowId" : "63eded4723be40ea9852610a72b09655",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "3c4f59c6b88846a9a6db8afef7ed8a7f",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-12-06T05:37:31.734542Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/neo",
  "jobData" : {}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('63eded4723be40ea9852610a72b09655')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








