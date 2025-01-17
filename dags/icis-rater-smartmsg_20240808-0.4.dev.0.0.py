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
                , WORKFLOW_NAME='smartmsg_20240808',WORKFLOW_ID='ad9b51dfb85049d39235bb7cf5621ddf', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-smartmsg_20240808-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 8, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ad9b51dfb85049d39235bb7cf5621ddf')

    sectTarifJob_vol = []
    sectTarifJob_volMnt = []
    sectTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    sectTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    sectTarifJob_env = [getICISConfigMap('icis-rater-batch-smartmsg-configmap'), getICISConfigMap('icis-rater-batch-smartmsg-configmap2'), getICISSecret('icis-rater-batch-smartmsg-secret')]
    sectTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    sectTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    sectTarifJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ace6115675ed4bee967b9ee096ef8095',
        'volumes': sectTarifJob_vol,
        'volume_mounts': sectTarifJob_volMnt,
        'env_from':sectTarifJob_env,
        'task_id':'sectTarifJob',
        'image':'/icis/icis-rater-batch-smartmsg:20240808101309',
        'arguments':["--job.names=sectTarifJob", "runType=T", "cyclYm=202405"
],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ad9b51dfb85049d39235bb7cf5621ddf')

    authCheck >> sectTarifJob >> Complete
    








