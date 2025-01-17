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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg_custno',WORKFLOW_ID='3c1748c05a854267a4a40de7d92291e8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg_custno-0.4.dev.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 5, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3c1748c05a854267a4a40de7d92291e8')

    otcWrlinCustNoDatCretJob_vol = []
    otcWrlinCustNoDatCretJob_volMnt = []
    otcWrlinCustNoDatCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    otcWrlinCustNoDatCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    otcWrlinCustNoDatCretJob_env = [getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISConfigMap('icis-rater-batch-otcorrg-configmap2'), getICISSecret('icis-rater-batch-otcorrg-secret')]
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    otcWrlinCustNoDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    otcWrlinCustNoDatCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0f8f4ebe9ef3414caa415d0582f3fe7f',
        'volumes': otcWrlinCustNoDatCretJob_vol,
        'volume_mounts': otcWrlinCustNoDatCretJob_volMnt,
        'env_from':otcWrlinCustNoDatCretJob_env,
        'task_id':'otcWrlinCustNoDatCretJob',
        'image':'/icis/icis-rater-batch-otcorrg:20240805165530',
        'arguments':["--job.names=otcWrlinCustNoDatCretJob", "runType=T", "cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3c1748c05a854267a4a40de7d92291e8')

    authCheck >> otcWrlinCustNoDatCretJob >> Complete
    








