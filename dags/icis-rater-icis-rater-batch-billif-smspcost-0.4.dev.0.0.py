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
                , WORKFLOW_NAME='icis-rater-batch-billif-smspcost',WORKFLOW_ID='548e0dd67096494fa2c168dff1f21b1f', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-billif-smspcost-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 28, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('548e0dd67096494fa2c168dff1f21b1f')

    smsPcostJob_vol = []
    smsPcostJob_volMnt = []
    smsPcostJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    smsPcostJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    smsPcostJob_env = [getICISConfigMap('icis-rater-batch-billif-configmap'), getICISConfigMap('icis-rater-batch-billif-configmap2'), getICISSecret('icis-rater-batch-billif-secret')]
    smsPcostJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    smsPcostJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    smsPcostJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6ec285de0bb04503b4af13ffdff6d8c0',
        'volumes': smsPcostJob_vol,
        'volume_mounts': smsPcostJob_volMnt,
        'env_from':smsPcostJob_env,
        'task_id':'smsPcostJob',
        'image':'/icis/icis-rater-batch-billif:20240729104230',
        'arguments':["--job.names=smsPcostJob", "runType=T", "cyclYm=202311"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('548e0dd67096494fa2c168dff1f21b1f')

    authCheck >> smsPcostJob >> Complete
    








