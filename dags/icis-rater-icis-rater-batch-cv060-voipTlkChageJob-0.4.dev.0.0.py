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
                , WORKFLOW_NAME='icis-rater-batch-cv060-voipTlkChageJob',WORKFLOW_ID='493455a657e346069eff8b40c5714cc3', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv060-voipTlkChageJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('493455a657e346069eff8b40c5714cc3')

    voipTlkChageJob_vol = []
    voipTlkChageJob_volMnt = []
    voipTlkChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    voipTlkChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    voipTlkChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    voipTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    voipTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    voipTlkChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '37bf2c4ea9f7477485fe3be4023188c5',
        'volumes': voipTlkChageJob_vol,
        'volume_mounts': voipTlkChageJob_volMnt,
        'env_from':voipTlkChageJob_env,
        'task_id':'voipTlkChageJob',
        'image':'/icis/icis-rater-batch-cv060:0.4.0.2',
        'arguments':["--job.names=voipTlkChageJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('493455a657e346069eff8b40c5714cc3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        voipTlkChageJob,
        Complete
    ]) 

    # authCheck >> voipTlkChageJob >> Complete
    workflow








