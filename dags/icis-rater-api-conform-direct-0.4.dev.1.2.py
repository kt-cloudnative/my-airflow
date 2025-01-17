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
                , WORKFLOW_NAME='api-conform-direct',WORKFLOW_ID='1a1bbc2baa7c43d9b0d88f9dc8cd6b37', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api-conform-direct-0.4.dev.1.2'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 20, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1a1bbc2baa7c43d9b0d88f9dc8cd6b37')


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': '05abe87bcbbc43f8bc0e202cf6ac473b',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.2",
  "wflowId" : "1a1bbc2baa7c43d9b0d88f9dc8cd6b37",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "05abe87bcbbc43f8bc0e202cf6ac473b",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-12-06T06:11:29.859393Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-engine-hotbill-server/hotbill/tibero",
  "jobData" : {    "inPayLoad": {        "err": "false"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': 'a9f13c936fdd4719ad4e28e528da6513',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.2",
  "wflowId" : "1a1bbc2baa7c43d9b0d88f9dc8cd6b37",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "a9f13c936fdd4719ad4e28e528da6513",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-12-06T06:11:29.861608Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/tibero",
  "jobData" : {}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': 'ef6924ddfbbf452cb543d7894bf3c92f',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.2",
  "wflowId" : "1a1bbc2baa7c43d9b0d88f9dc8cd6b37",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "ef6924ddfbbf452cb543d7894bf3c92f",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-12-06T06:11:29.862984Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/neo",
  "jobData" : {}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('1a1bbc2baa7c43d9b0d88f9dc8cd6b37')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








