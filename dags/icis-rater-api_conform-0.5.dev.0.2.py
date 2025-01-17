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
                , WORKFLOW_NAME='api_conform',WORKFLOW_ID='309d15744ea84871b3f264ddefc2c688', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api_conform-0.5.dev.0.2'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 6, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('309d15744ea84871b3f264ddefc2c688')


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': 'ceac564ede204b458de4329911ad5fc4',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.2",
  "wflowId" : "309d15744ea84871b3f264ddefc2c688",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "ceac564ede204b458de4329911ad5fc4",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-11-06T09:06:03.134265Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/tibero",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/tibero",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': 'd79c071f291848ab8adeb8ee525682be',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.2",
  "wflowId" : "309d15744ea84871b3f264ddefc2c688",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "d79c071f291848ab8adeb8ee525682be",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-11-06T09:06:03.132608Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/hotbill/au",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/hotbill/au",        "cbFnName": "service"    },    "rtAuInDto": {        "svcContId": "11750267841",        "billYear": "${YYYY}",        "billMonth": "${MM}",        "wrkDivCd": "3"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': '344150b909474bb1a7d0a1a25354ee59',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.2",
  "wflowId" : "309d15744ea84871b3f264ddefc2c688",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "344150b909474bb1a7d0a1a25354ee59",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-11-06T09:06:03.123651Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/neo",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/neo",        "cbFnName": "service"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('309d15744ea84871b3f264ddefc2c688')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








