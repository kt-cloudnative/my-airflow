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
                , WORKFLOW_NAME='icis-rater-batch-cv060-wrlinTlkChageJob',WORKFLOW_ID='b31d785b821d4d58aee826201d6ae881', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cv060-wrlinTlkChageJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b31d785b821d4d58aee826201d6ae881')

    wrlinTlkChageJob_vol = []
    wrlinTlkChageJob_volMnt = []
    wrlinTlkChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    wrlinTlkChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    wrlinTlkChageJob_env = [getICISConfigMap('icis-rater-batch-cv060-configmap'), getICISConfigMap('icis-rater-batch-cv060-configmap2'), getICISSecret('icis-rater-batch-cv060-secret')]
    wrlinTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    wrlinTlkChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    wrlinTlkChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b9ec422d5d744a7fb7b9b66995403446',
        'volumes': wrlinTlkChageJob_vol,
        'volume_mounts': wrlinTlkChageJob_volMnt,
        'env_from':wrlinTlkChageJob_env,
        'task_id':'wrlinTlkChageJob',
        'image':'/icis/icis-rater-batch-cv060:0.4.0.4',
        'arguments':["--job.names=wrlinTlkChageJob", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b31d785b821d4d58aee826201d6ae881')

    workflow = COMMON.getICISPipeline([
        authCheck,
        wrlinTlkChageJob,
        Complete
    ]) 

    # authCheck >> wrlinTlkChageJob >> Complete
    workflow








