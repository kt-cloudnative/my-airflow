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
                , WORKFLOW_NAME='api-conform-direct',WORKFLOW_ID='7f4ea56d1f6848298136a7efee819d2e', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api-conform-direct-0.4.dev.1.0'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 20, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7f4ea56d1f6848298136a7efee819d2e')


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': 'c4a782b04cd2425bbb29d28e27e01226',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.0",
  "wflowId" : "7f4ea56d1f6848298136a7efee819d2e",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "c4a782b04cd2425bbb29d28e27e01226",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-11-20T04:19:10.990221Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-engine-hotbill-server/hotbill/tibero",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/hotbill/au",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/hotbill/au",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': 'f6b77399ea7f4806b467b9f0905894e4',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.0",
  "wflowId" : "7f4ea56d1f6848298136a7efee819d2e",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "f6b77399ea7f4806b467b9f0905894e4",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-11-20T04:19:10.987418Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/neo",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/neo",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/neo",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': 'a86654a034db4df399b746eae47c7fba',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-rater-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.4.dev.1.0",
  "wflowId" : "7f4ea56d1f6848298136a7efee819d2e",
  "wflowNm" : "api-conform-direct",
  "wflowTaskId" : "a86654a034db4df399b746eae47c7fba",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-11-20T04:19:10.991587Z",
  "cretId" : "82265574",
  "endpoint" : "http://icis-rater-uq-apisvc2/apisvc2/tibero",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/tibero",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/tibero",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('7f4ea56d1f6848298136a7efee819d2e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








