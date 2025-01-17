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
                , WORKFLOW_NAME='api_conform',WORKFLOW_ID='3a3151efcac043259435d48febd46254', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-api_conform-0.5.dev.0.0'
    ,'schedule_interval':'0 8 * * * '
    ,'start_date': datetime(2024, 11, 4, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3a3151efcac043259435d48febd46254')


      
       
    uqApiCall = COMMON.getAgentHttpOperator({
        'id': '140cd69d764e46cfb2587d5d2c892229',
        'task_id': 'uqApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.0",
  "wflowId" : "3a3151efcac043259435d48febd46254",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "140cd69d764e46cfb2587d5d2c892229",
  "taskId" : "uqApiCall",
  "cretDt" : "2024-11-04T05:41:34.459801Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/common/usage",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/common/usage",        "cbFnName": "service"    },    "trapi101InQryDto": {        "dbRetvSeq": "0",        "dbRetvCascnt": "10",        "vcChrgGrpId": "8900",        "vcBillYm": "${YYYYMM}",        "svcContId": "11650075371"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    uqApiNeoCall = COMMON.getAgentHttpOperator({
        'id': '82ce66fcf3b5412091dcc7d474837a60',
        'task_id': 'uqApiNeoCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.0",
  "wflowId" : "3a3151efcac043259435d48febd46254",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "82ce66fcf3b5412091dcc7d474837a60",
  "taskId" : "uqApiNeoCall",
  "cretDt" : "2024-11-04T05:41:34.483949Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/apisvc2/common/usage",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/apisvc2/common/usage",        "cbFnName": "service"    },    "trapi101InQryDto": {        "dbRetvSeq": "0",        "dbRetvCascnt": "10",        "vcChrgGrpId": "8900",        "vcBillYm": "${YYYYMM}",        "svcContId": "11650075371"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
       
    hotbillApiCall = COMMON.getAgentHttpOperator({
        'id': '286246032b34490caed9f3ff71663ad0',
        'task_id': 'hotbillApiCall',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.5.dev.0.0",
  "wflowId" : "3a3151efcac043259435d48febd46254",
  "wflowNm" : "api_conform",
  "wflowTaskId" : "286246032b34490caed9f3ff71663ad0",
  "taskId" : "hotbillApiCall",
  "cretDt" : "2024-11-04T05:41:34.487318Z",
  "cretId" : "82265574",
  "endpoint" : "http://rest-gw-rater.dev.icis.kt.co.kr/json",
  "jobData" : {    "commonHeader": {        "appName": "NBSS_TRAT",        "svcName": "/hotbill/au",        "fnName": "service",        "globalNo": "09133791020230721102817247733153",        "chnlType": "",        "trFlag": "",        "trDate": "",        "trTime": "",        "clntIp": "",        "userId": "91337910",        "realUserId": "91337910",        "orgId": "",        "srcId": "",        "curHostId": "",        "lgDateTime": "",        "cmpnCd": "KT"    },    "bizHeader": {        "cbSvcName": "/hotbill/au",        "cbFnName": "service"    },    "rtAuInDto": {        "svcContId": "11750267841",        "billYear": "${YYYY}",        "billMonth": "${MM}",        "wrkDivCd": "3"    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      

    Complete = COMMON.getICISCompleteWflowTask('3a3151efcac043259435d48febd46254')

    workflow = COMMON.getICISPipeline([
        authCheck,
        uqApiCall,
        uqApiNeoCall,
        hotbillApiCall,
        Complete
    ]) 

    # authCheck >> uqApiCall>>uqApiNeoCall>>hotbillApiCall >> Complete
    workflow








