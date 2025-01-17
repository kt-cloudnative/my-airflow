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
                , WORKFLOW_NAME='dscsTarifJob',WORKFLOW_ID='11abfccf78984878bb87acccea54d9e8', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-dscsTarifJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 28, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('11abfccf78984878bb87acccea54d9e8')

    dscsTarifJob_vol = []
    dscsTarifJob_volMnt = []
    dscsTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    dscsTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    dscsTarifJob_env = [getICISConfigMap('icis-rater-batch-smartmsg-configmap'), getICISConfigMap('icis-rater-batch-smartmsg-configmap2'), getICISSecret('icis-rater-batch-smartmsg-secret')]
    dscsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    dscsTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    dscsTarifJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '376562334907445387bfcba8f854ff22',
        'volumes': dscsTarifJob_vol,
        'volume_mounts': dscsTarifJob_volMnt,
        'env_from':dscsTarifJob_env,
        'task_id':'dscsTarifJob',
        'image':'/icis/icis-rater-batch-smartmsg:20240828142508',
        'arguments':["--job.names=dscsTarifJob", "runType=T", "cyclYm=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('11abfccf78984878bb87acccea54d9e8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        dscsTarifJob,
        Complete
    ]) 

    # authCheck >> dscsTarifJob >> Complete
    workflow








