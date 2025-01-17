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
                , WORKFLOW_NAME='sectTarif_202410_v2',WORKFLOW_ID='1eafea99ac794f1a82b7936f652263b1', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-sectTarif_202410_v2-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 12, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1eafea99ac794f1a82b7936f652263b1')

    sectTarifJob_vol = []
    sectTarifJob_volMnt = []
    sectTarifJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    sectTarifJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    sectTarifJob_env = [getICISConfigMap('icis-rater-batch-smartmsg-configmap'), getICISConfigMap('icis-rater-batch-smartmsg-configmap2'), getICISSecret('icis-rater-batch-smartmsg-secret')]
    sectTarifJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    sectTarifJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    sectTarifJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b8e926a82d66415589efe63044384064',
        'volumes': sectTarifJob_vol,
        'volume_mounts': sectTarifJob_volMnt,
        'env_from':sectTarifJob_env,
        'task_id':'sectTarifJob',
        'image':'/icis/icis-rater-batch-smartmsg:20241212183123',
        'arguments':["--job.names=sectTarifJob", "runType=T", "cyclYm=202410"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1eafea99ac794f1a82b7936f652263b1')

    workflow = COMMON.getICISPipeline([
        authCheck,
        sectTarifJob,
        Complete
    ]) 

    # authCheck >> sectTarifJob >> Complete
    workflow








