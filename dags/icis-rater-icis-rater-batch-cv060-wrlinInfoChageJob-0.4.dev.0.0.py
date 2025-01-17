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
                , WORKFLOW_NAME='icis-rater-batch-cv060-wrlinInfoChageJob',WORKFLOW_ID='ffdf0660127149d7a1f1859ac388908e', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv060-wrlinInfoChageJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ffdf0660127149d7a1f1859ac388908e')

    wrlinInfoChageJob_vol = []
    wrlinInfoChageJob_volMnt = []
    wrlinInfoChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    wrlinInfoChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    wrlinInfoChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    wrlinInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    wrlinInfoChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    wrlinInfoChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b7894d54294c4b9c88de4af37d427148',
        'volumes': wrlinInfoChageJob_vol,
        'volume_mounts': wrlinInfoChageJob_volMnt,
        'env_from':wrlinInfoChageJob_env,
        'task_id':'wrlinInfoChageJob',
        'image':'/icis/icis-rater-batch-cv060:0.4.0.4',
        'arguments':["--job.names=wrlinInfoChageJob", "cyclYm=202405"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ffdf0660127149d7a1f1859ac388908e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        wrlinInfoChageJob,
        Complete
    ]) 

    # authCheck >> wrlinInfoChageJob >> Complete
    workflow








