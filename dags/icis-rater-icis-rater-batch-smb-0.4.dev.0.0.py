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
                , WORKFLOW_NAME='icis-rater-batch-smb',WORKFLOW_ID='1fd353c4c57249a483d09226d447b7d8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-smb-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 22, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1fd353c4c57249a483d09226d447b7d8')

    smbJob_vol = []
    smbJob_volMnt = []
    smbJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    smbJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    smbJob_env = [getICISConfigMap('icis-rater-batch-smb-configmap'), getICISConfigMap('icis-rater-batch-smb-configmap2'), getICISSecret('icis-rater-batch-smb-secret')]
    smbJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    smbJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    smbJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6b7212989208469aa20dd65bd5425a2b',
        'volumes': smbJob_vol,
        'volume_mounts': smbJob_volMnt,
        'env_from':smbJob_env,
        'task_id':'smbJob',
        'image':'/icis/icis-rater-batch-smb:20240923140359',
        'arguments':["--job.names=smbJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1fd353c4c57249a483d09226d447b7d8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        smbJob,
        Complete
    ]) 

    # authCheck >> smbJob >> Complete
    workflow








