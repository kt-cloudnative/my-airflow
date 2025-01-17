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
                , WORKFLOW_NAME='dscsTarifJob_07_20240828',WORKFLOW_ID='fc0e8b00310c4d888f5029f39b5b7593', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-dscsTarifJob_07_20240828-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 28, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fc0e8b00310c4d888f5029f39b5b7593')

    dscsTarifJob_vol = []
    dscsTarifJob_volMnt = []
    dscsTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    dscsTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    dscsTarifJob_env = [getICISConfigMap('icis-rater-batch-smartmsg-configmap'), getICISConfigMap('icis-rater-batch-smartmsg-configmap2'), getICISSecret('icis-rater-batch-smartmsg-secret')]
    dscsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    dscsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    dscsTarifJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '447d18eafe534ad3b1a8af460627e1ce',
        'volumes': dscsTarifJob_vol,
        'volume_mounts': dscsTarifJob_volMnt,
        'env_from':dscsTarifJob_env,
        'task_id':'dscsTarifJob',
        'image':'/icis/icis-rater-batch-smartmsg:20240828174754',
        'arguments':["--job.names=dscsTarifJob", "runType=T", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fc0e8b00310c4d888f5029f39b5b7593')

    workflow = COMMON.getICISPipeline([
        authCheck,
        dscsTarifJob,
        Complete
    ]) 

    # authCheck >> dscsTarifJob >> Complete
    workflow








