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
                , WORKFLOW_NAME='icis-rater-batch-cvrepno-voip',WORKFLOW_ID='da02839c78e4452585ecd887499c9c07', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvrepno-voip-0.2.dev.3.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 7, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('da02839c78e4452585ecd887499c9c07')

    cvRepNoVoIPJob_vol = []
    cvRepNoVoIPJob_volMnt = []
    cvRepNoVoIPJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvRepNoVoIPJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvRepNoVoIPJob_env = [getICISConfigMap('icis-rater-batch-cvrepno-configmap'), getICISConfigMap('icis-rater-batch-cvrepno-configmap2'), getICISSecret('icis-rater-batch-cvrepno-secret')]
    cvRepNoVoIPJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvRepNoVoIPJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvRepNoVoIPJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a1baaee0f624e48b8ba022fae59a6f5',
        'volumes': cvRepNoVoIPJob_vol,
        'volume_mounts': cvRepNoVoIPJob_volMnt,
        'env_from':cvRepNoVoIPJob_env,
        'task_id':'cvRepNoVoIPJob',
        'image':'/icis/icis-rater-batch-cvrepno:20240401142152',
        'arguments':["--job.names=cvRepNoVoIPJob", "runType=T", "cyclYm=202404"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('da02839c78e4452585ecd887499c9c07')

    authCheck >> cvRepNoVoIPJob >> Complete
    








