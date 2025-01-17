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
                , WORKFLOW_NAME='icis-rater-batch-morrg-mnpTmon',WORKFLOW_ID='39bb6f19e52d4ef8aa5b4874bd884274', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-morrg-mnpTmon-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('39bb6f19e52d4ef8aa5b4874bd884274')

    mnpTmonDatCretJob_vol = []
    mnpTmonDatCretJob_volMnt = []
    mnpTmonDatCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    mnpTmonDatCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    mnpTmonDatCretJob_env = [getICISConfigMap('icis-rater-batch-morrg-configmap'), getICISConfigMap('icis-rater-batch-morrg-configmap2'), getICISSecret('icis-rater-batch-morrg-secret')]
    mnpTmonDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    mnpTmonDatCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    mnpTmonDatCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '17f8bdf7bfc14c7095d1695a3cd79bb2',
        'volumes': mnpTmonDatCretJob_vol,
        'volume_mounts': mnpTmonDatCretJob_volMnt,
        'env_from':mnpTmonDatCretJob_env,
        'task_id':'mnpTmonDatCretJob',
        'image':'/icis/icis-rater-batch-morrg:20241021154344',
        'arguments':["--job.names=mnpTmonDatCretJob", "cyclYm=202412"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('39bb6f19e52d4ef8aa5b4874bd884274')

    workflow = COMMON.getICISPipeline([
        authCheck,
        mnpTmonDatCretJob,
        Complete
    ]) 

    # authCheck >> mnpTmonDatCretJob >> Complete
    workflow








