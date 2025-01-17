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
                , WORKFLOW_NAME='kdapUsMonthGrp3',WORKFLOW_ID='e5326e0d64e94c5a9c8a23a8e5f2aff1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-kdapUsMonthGrp3-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 12, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e5326e0d64e94c5a9c8a23a8e5f2aff1')

    kdapUsJob_vol = []
    kdapUsJob_volMnt = []
    kdapUsJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kdapUsJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kdapUsJob_env = [getICISConfigMap('icis-rater-batch-kdap-configmap'), getICISConfigMap('icis-rater-batch-kdap-configmap2'), getICISSecret('icis-rater-batch-kdap-secret')]
    kdapUsJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kdapUsJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kdapUsJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8890c291a01444f48ce39c83ba289e81',
        'volumes': kdapUsJob_vol,
        'volume_mounts': kdapUsJob_volMnt,
        'env_from':kdapUsJob_env,
        'task_id':'kdapUsJob',
        'image':'/icis/icis-rater-batch-kdap:0.4.0.16',
        'arguments':["--job.names=kdapUsJob", "retvType=M", "cyclYm=202407",  "cyclGroup=3"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e5326e0d64e94c5a9c8a23a8e5f2aff1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kdapUsJob,
        Complete
    ]) 

    # authCheck >> kdapUsJob >> Complete
    workflow








