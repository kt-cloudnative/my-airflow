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
                , WORKFLOW_NAME='icis-rater-batch-billif-cardtlk',WORKFLOW_ID='642830c56cc74375a3131202bac14d5a', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-billif-cardtlk-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('642830c56cc74375a3131202bac14d5a')

    cardTlkTxnJob_vol = []
    cardTlkTxnJob_volMnt = []
    cardTlkTxnJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cardTlkTxnJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cardTlkTxnJob_env = [getICISConfigMap('icis-rater-batch-billif-configmap'), getICISConfigMap('icis-rater-batch-billif-configmap2'), getICISSecret('icis-rater-batch-billif-secret')]
    cardTlkTxnJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cardTlkTxnJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cardTlkTxnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '85033752d787471dbfaf9091f229d407',
        'volumes': cardTlkTxnJob_vol,
        'volume_mounts': cardTlkTxnJob_volMnt,
        'env_from':cardTlkTxnJob_env,
        'task_id':'cardTlkTxnJob',
        'image':'/icis/icis-rater-batch-billif:20240821111138',
        'arguments':["--job.names=cardTlkTxnJob", "cyclYm=202402"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('642830c56cc74375a3131202bac14d5a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cardTlkTxnJob,
        Complete
    ]) 

    # authCheck >> cardTlkTxnJob >> Complete
    workflow








