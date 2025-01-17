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
                , WORKFLOW_NAME='udlee-airflow-0511-2',WORKFLOW_ID='4e5c5535cab34aaa816877ccd42fe661', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-udlee-airflow-0511-2-0.0.dev.0.0'
    ,'schedule_interval':'15 4 * * 6'
    ,'start_date': datetime(2024, 5, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4e5c5535cab34aaa816877ccd42fe661')

    testPod_vol = []
    testPod_volMnt = []
    testPod_env = [getICISConfigMap('icis-rater-glenrater05102-configmap'), getICISConfigMap('icis-rater-glenrater05102-configmap2'), getICISSecret('icis-rater-glenrater05102-secret')]
    testPod_env.extend([getICISConfigMap('icis-rater-cmmn-configmap'), getICISSecret('icis-rater-cmmn-secret')])
    testPod_env.extend([getICISConfigMap('icis-rater-truststore.jks')])

    testPod = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '657cf7abe52946dc915aa98b1dcb5a1a',
        'volumes': testPod_vol,
        'volume_mounts': testPod_volMnt,
        'env_from':testPod_env,
        'task_id':'testPod',
        'image':'/icis/icis-rater-glenrater05102:20240511040134',
        'arguments':["--job.names=sampJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


    testHttp = COMMON.getICISSimpleHttpOperator_v1({
        'id' : '04696be5a9e7432a916d46be9af7bd15',
        'task_id' : 'testHttp',
        'method' : 'POST',
        'endpoint' : 'icis-samp-ppon.dev.icis.kt.co.kr/ppon/cont/retvById',
        'headers' : {
    "Content-Type": "application/json"
},
        'data' : {
    "pponContPayload": {
        "saPponContNo": 10,
        "prevSaPponContHstNo": 3
    }
},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })
      
       
      


      
       
      
    testVrf = COMMON.getAgentVrfOperator({
        'id' : '9c76b53d25a241e1ba11afa6443440da',
        'task_id' : 'testVrf',
        'endpoint' : 'icis-cmmn-batchcommander.dev.icis.kt.co.kr',
        'data' : {
  "batchAgentUrl" : "http://icis-oder-batchagent.dev.icis.kt.co.kr",
  "wflowVer" : "0.0.dev.0.0",
  "wflowId" : "4e5c5535cab34aaa816877ccd42fe661",
  "wflowNm" : "udlee-airflow-0511-2",
  "wflowTaskId" : "9c76b53d25a241e1ba11afa6443440da",
  "taskId" : "testVrf",
  "cretDt" : "2024-05-10T19:12:47Z",
  "cretId" : "91319392",
  "useSkip" : "N",
  "vrfType" : "==",
  "vrfDiv1" : "TEXT",
  "vrfCmd1" : "상어",
  "vrfDiv2" : "QUERY",
  "vrfCmd2" : "select '상어' from dual"
},
        'taskAlrmSucesYn': 'N', # 성공 알림 전송
        'taskAlrmFailYn': 'N'  # 실패 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('4e5c5535cab34aaa816877ccd42fe661')

    authCheck >> testPod >> testHttp >> testVrf >> Complete
    








