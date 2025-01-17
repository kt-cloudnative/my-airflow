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
                , WORKFLOW_NAME='icis-rater-batch-lmtchk',WORKFLOW_ID='d2a27f8184954d28b5e7fbc4a515335b', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-lmtchk-0.4.dev.6.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 1, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d2a27f8184954d28b5e7fbc4a515335b')

    lmtAmtExcsFileJob_vol = []
    lmtAmtExcsFileJob_volMnt = []
    lmtAmtExcsFileJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lmtAmtExcsFileJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lmtAmtExcsFileJob_env = [getICISConfigMap('icis-rater-batch-lmtchk-configmap'), getICISConfigMap('icis-rater-batch-lmtchk-configmap2'), getICISSecret('icis-rater-batch-lmtchk-secret')]
    lmtAmtExcsFileJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lmtAmtExcsFileJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lmtAmtExcsFileJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '00ca895a9b854767afd399ead945d7a1',
        'volumes': lmtAmtExcsFileJob_vol,
        'volume_mounts': lmtAmtExcsFileJob_volMnt,
        'env_from':lmtAmtExcsFileJob_env,
        'task_id':'lmtAmtExcsFileJob',
        'image':'/icis/icis-rater-batch-lmtchk:20240802093140',
        'arguments':["--job.names=lmtAmtExcsFileJob", "clstrDt=20240701"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d2a27f8184954d28b5e7fbc4a515335b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        lmtAmtExcsFileJob,
        Complete
    ]) 

    # authCheck >> lmtAmtExcsFileJob >> Complete
    workflow








