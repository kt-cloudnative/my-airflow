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
                , WORKFLOW_NAME='wf-extr-rater',WORKFLOW_ID='ff7fbe0b585b490883f3e5cca2ea6302', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-wf-extr-rater-0.0.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 5, 9, 0, 4, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ff7fbe0b585b490883f3e5cca2ea6302')


      
       
    http = COMMON.getAgentHttpOperator({
        'id': '7a4b02492ce94721b12e3b14c632daec',
        'task_id': 'http',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {
    "Content-Type": "application/json"
},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.0.dev.0.0",
  "wflowId" : "ff7fbe0b585b490883f3e5cca2ea6302",
  "wflowNm" : "wf-extr-rater",
  "wflowTaskId" : "7a4b02492ce94721b12e3b14c632daec",
  "taskId" : "http",
  "cretDt" : "2024-05-10T18:31:03Z",
  "cretId" : "91318962",
  "endpoint" : "icis-samp-ppon.dev.icis.kt.co.kr/ppon/cont/retvById",
  "jobData" : {    "pponContPayload": {        "saPponContNo": 10,        "prevSaPponContHstNo": 3    }}
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      


      
    shell = COMMON.getAgentShellOperator({
        'id': '30e28b304e544c64b15dbe21ff1c2e10',
        'task_id': 'shell',
        'method' : 'POST',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'headers' : {"content-type": "application/json"},
        'data': {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.0.dev.0.0",
  "wflowId" : "ff7fbe0b585b490883f3e5cca2ea6302",
  "wflowNm" : "wf-extr-rater",
  "wflowTaskId" : "30e28b304e544c64b15dbe21ff1c2e10",
  "taskId" : "shell",
  "cretDt" : "2024-05-10T18:31:03Z",
  "cretId" : "91318962",
  "endpoint" : "./app/resources/shell/test.sh",
  "jobData" : "3232323"
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
       
      


      
       
      
    vrf = COMMON.getAgentVrfOperator({
        'id' : '195e08101c8649cfbfb0c8380b5f0091',
        'task_id' : 'vrf',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'data' : {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.0.dev.0.0",
  "wflowId" : "ff7fbe0b585b490883f3e5cca2ea6302",
  "wflowNm" : "wf-extr-rater",
  "wflowTaskId" : "195e08101c8649cfbfb0c8380b5f0091",
  "taskId" : "vrf",
  "cretDt" : "2024-05-10T18:31:03Z",
  "cretId" : "91318962",
  "useSkip" : "N",
  "vrfType" : "==",
  "vrfDiv1" : "QUERY",
  "vrfCmd1" : "select '상어' from dual",
  "vrfDiv2" : "QUERY",
  "vrfCmd2" : "select '상어' from dual"
},
        'taskAlrmSucesYn': 'N', # 성공 알림 전송
        'taskAlrmFailYn': 'N'  # 실패 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('ff7fbe0b585b490883f3e5cca2ea6302')

    authCheck >> http >> shell >> vrf >> Complete
    








