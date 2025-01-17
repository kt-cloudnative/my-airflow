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
                , WORKFLOW_NAME='icis-rater-batch-morrg-mnp',WORKFLOW_ID='11fa9edc08b14691aea4062eb1b5e13e', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-morrg-mnp-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 22, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('11fa9edc08b14691aea4062eb1b5e13e')

    mnpOrrgCretJob_vol = []
    mnpOrrgCretJob_volMnt = []
    mnpOrrgCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    mnpOrrgCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    mnpOrrgCretJob_env = [getICISConfigMap('icis-rater-batch-morrg-configmap'), getICISConfigMap('icis-rater-batch-morrg-configmap2'), getICISSecret('icis-rater-batch-morrg-secret')]
    mnpOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    mnpOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    mnpOrrgCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fccc135bf4df482fac57c04d78963004',
        'volumes': mnpOrrgCretJob_vol,
        'volume_mounts': mnpOrrgCretJob_volMnt,
        'env_from':mnpOrrgCretJob_env,
        'task_id':'mnpOrrgCretJob',
        'image':'/icis/icis-rater-batch-morrg:20241023133907',
        'arguments':["--job.names=mnpOrrgCretJob", "trtYmd=20240804"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('11fa9edc08b14691aea4062eb1b5e13e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        mnpOrrgCretJob,
        Complete
    ]) 

    # authCheck >> mnpOrrgCretJob >> Complete
    workflow








