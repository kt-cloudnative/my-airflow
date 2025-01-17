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
                , WORKFLOW_NAME='rater_rerate_steptarif_ke',WORKFLOW_ID='13d02d7b68284021a3ab34c7e9d857f1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-rater_rerate_steptarif_ke-0.4.dev.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 25, 0, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('13d02d7b68284021a3ab34c7e9d857f1')

    stepTarifJob_vol = []
    stepTarifJob_volMnt = []
    stepTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    stepTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    stepTarifJob_env = [getICISConfigMap('icis-rater-batch-rerate-configmap'), getICISConfigMap('icis-rater-batch-rerate-configmap2'), getICISSecret('icis-rater-batch-rerate-secret')]
    stepTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    stepTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    stepTarifJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '00d373b96db9438ba97725b1965715c7',
        'volumes': stepTarifJob_vol,
        'volume_mounts': stepTarifJob_volMnt,
        'env_from':stepTarifJob_env,
        'task_id':'stepTarifJob',
        'image':'/icis/icis-rater-batch-rerate:20240823181858',
        'arguments':["--job.names=stepTarifJob", "runType=T", "cyclYm=202403", "format=KE"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('13d02d7b68284021a3ab34c7e9d857f1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        stepTarifJob,
        Complete
    ]) 

    # authCheck >> stepTarifJob >> Complete
    workflow








