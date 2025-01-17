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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg_custNo',WORKFLOW_ID='2e51f03bf97141cca8e8b6bb1fa51721', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg_custNo-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 22, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2e51f03bf97141cca8e8b6bb1fa51721')

    otcWrlinCustNoDatCretJob_vol = []
    otcWrlinCustNoDatCretJob_volMnt = []
    otcWrlinCustNoDatCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    otcWrlinCustNoDatCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    otcWrlinCustNoDatCretJob_env = [getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISConfigMap('icis-rater-batch-otcorrg-configmap2'), getICISSecret('icis-rater-batch-otcorrg-secret')]
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcWrlinCustNoDatCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f5e1a103d37a4e6188edd21510136f69',
        'volumes': otcWrlinCustNoDatCretJob_vol,
        'volume_mounts': otcWrlinCustNoDatCretJob_volMnt,
        'env_from':otcWrlinCustNoDatCretJob_env,
        'task_id':'otcWrlinCustNoDatCretJob',
        'image':'/icis/icis-rater-batch-otcorrg:20240722153638',
        'arguments':["--job.names=otcWrlinCustNoDatCretJob", "runType=T", "cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2e51f03bf97141cca8e8b6bb1fa51721')

    authCheck >> otcWrlinCustNoDatCretJob >> Complete
    








