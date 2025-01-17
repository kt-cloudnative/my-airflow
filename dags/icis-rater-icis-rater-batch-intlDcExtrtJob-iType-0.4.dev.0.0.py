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
                , WORKFLOW_NAME='icis-rater-batch-intlDcExtrtJob-iType',WORKFLOW_ID='bf03bb6588184173b4997d8c14173cce', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intlDcExtrtJob-iType-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bf03bb6588184173b4997d8c14173cce')

    intlDcExtrtJob_vol = []
    intlDcExtrtJob_volMnt = []
    intlDcExtrtJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intlDcExtrtJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intlDcExtrtJob_env = [getICISConfigMap('icis-rater-batch-billif-configmap'), getICISConfigMap('icis-rater-batch-billif-configmap2'), getICISSecret('icis-rater-batch-billif-secret')]
    intlDcExtrtJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intlDcExtrtJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intlDcExtrtJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8ec08ef34ec64ae7ad38dac1430f39cb',
        'volumes': intlDcExtrtJob_vol,
        'volume_mounts': intlDcExtrtJob_volMnt,
        'env_from':intlDcExtrtJob_env,
        'task_id':'intlDcExtrtJob',
        'image':'/icis/icis-rater-batch-billif:0.4.0.22',
        'arguments':["--job.names=intlDcExtrtJob", "cyclYm=202403" ,"jobDivVal=I"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bf03bb6588184173b4997d8c14173cce')

    workflow = COMMON.getICISPipeline([
        authCheck,
        intlDcExtrtJob,
        Complete
    ]) 

    # authCheck >> intlDcExtrtJob >> Complete
    workflow








