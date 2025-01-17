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
                , WORKFLOW_NAME='api_conform',WORKFLOW_ID='8b5f4fee2a8f4ab9879e31e048275e74', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api_conform-0.5.dev.0.3'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 6, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8b5f4fee2a8f4ab9879e31e048275e74')


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': '86a9d865adf645e19071fc59fdce8e3f',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.3",
  "wflowId" : "8b5f4fee2a8f4ab9879e31e048275e74",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "86a9d865adf645e19071fc59fdce8e3f",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-11-06T09:07:34.436748Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/neo",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/neo",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': '5dc68511852f4aa6bece08c8cbacc9e8',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.3",
  "wflowId" : "8b5f4fee2a8f4ab9879e31e048275e74",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "5dc68511852f4aa6bece08c8cbacc9e8",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-11-06T09:07:34.435002Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/hotbill/au",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/hotbill/au",        "cbFnName": "service"    },    "rtAuBillInDto": {        "svcContId": "11750267841",        "billYear": "${YYYY}",        "billMonth": "${MM}",        "wrkDivCd": "3"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': '7f7553ff2d39442ab969c051aa676fbd',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.3",
  "wflowId" : "8b5f4fee2a8f4ab9879e31e048275e74",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "7f7553ff2d39442ab969c051aa676fbd",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-11-06T09:07:34.432935Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/tibero",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/tibero",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('8b5f4fee2a8f4ab9879e31e048275e74')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








