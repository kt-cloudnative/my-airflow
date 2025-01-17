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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg_fax114',WORKFLOW_ID='5aa65776790549f283f3078917461c45', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg_fax114-0.4.dev.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 9, 11, 14, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5aa65776790549f283f3078917461c45')

    otcWrlinCustNoDatCretJob_vol = []
    otcWrlinCustNoDatCretJob_volMnt = []
    otcWrlinCustNoDatCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    otcWrlinCustNoDatCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    otcWrlinCustNoDatCretJob_env = [getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret'), getICISConfigMap('icis-rater-batch-configmap'), getICISSecret('icis-rater-batch-secret')]
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-otcorrg-mng-configmap'), getICISSecret('icis-rater-batch-otcorrg-mng-secret'), getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISSecret('icis-rater-batch-otcorrg-secret')])
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcWrlinCustNoDatCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '88da63b2f19f4a508d3a7ba8db590332',
        'volumes': otcWrlinCustNoDatCretJob_vol,
        'volume_mounts': otcWrlinCustNoDatCretJob_volMnt,
        'env_from':otcWrlinCustNoDatCretJob_env,
        'task_id':'otcWrlinCustNoDatCretJob',
        'image':'/icis/icis-rater-batch-otcorrg:20241209190145',
        'arguments':["--job.names=otcWrlinFax114CretJob", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5aa65776790549f283f3078917461c45')

    workflow = COMMON.getICISPipeline([
        authCheck,
        otcWrlinCustNoDatCretJob,
        Complete
    ]) 

    # authCheck >> otcWrlinCustNoDatCretJob >> Complete
    workflow








