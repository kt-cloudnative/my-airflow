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
                , WORKFLOW_NAME='icis-rater-batch-cris-040',WORKFLOW_ID='7ce149d54c944910aa77007632dcc67c', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cris-040-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 19, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7ce149d54c944910aa77007632dcc67c')

    crisFileCretJob_vol = []
    crisFileCretJob_volMnt = []
    crisFileCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    crisFileCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    crisFileCretJob_env = [getICISConfigMap('icis-rater-batch-cris-configmap'), getICISConfigMap('icis-rater-batch-cris-configmap2'), getICISSecret('icis-rater-batch-cris-secret')]
    crisFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    crisFileCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    crisFileCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6419bb8a0b714fd6aea132a2419e9d64',
        'volumes': crisFileCretJob_vol,
        'volume_mounts': crisFileCretJob_volMnt,
        'env_from':crisFileCretJob_env,
        'task_id':'crisFileCretJob',
        'image':'/icis/icis-rater-batch-cris:20240819164618',
        'arguments':["--job.names=crisFileCretJob", "cyclYm=${YYYYMM}" , "cyclYm=${YYYYMM, MM, -01}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7ce149d54c944910aa77007632dcc67c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        crisFileCretJob,
        Complete
    ]) 

    # authCheck >> crisFileCretJob >> Complete
    workflow








