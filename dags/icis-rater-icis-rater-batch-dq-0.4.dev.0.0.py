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
                , WORKFLOW_NAME='icis-rater-batch-dq',WORKFLOW_ID='a16182151ece4c2789fd0e52e04386eb', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-dq-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a16182151ece4c2789fd0e52e04386eb')

    dqJob_vol = []
    dqJob_volMnt = []
    dqJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    dqJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    dqJob_env = [getICISConfigMap('icis-rater-batch-dq-configmap'), getICISConfigMap('icis-rater-batch-dq-configmap2'), getICISSecret('icis-rater-batch-dq-secret')]
    dqJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    dqJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    dqJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '74761061e65b43ca9e6ebe214c58c1ea',
        'volumes': dqJob_vol,
        'volume_mounts': dqJob_volMnt,
        'env_from':dqJob_env,
        'task_id':'dqJob',
        'image':'/icis/icis-rater-batch-dq:0.4.0.3',
        'arguments':["--job.names=dqJob", "dqId=IR0002", "fileNmDate=20240701" ,"cdrFileFmtId=IF"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a16182151ece4c2789fd0e52e04386eb')

    workflow = COMMON.getICISPipeline([
        authCheck,
        dqJob,
        Complete
    ]) 

    # authCheck >> dqJob >> Complete
    workflow








