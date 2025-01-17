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
                , WORKFLOW_NAME='icis-rater-batch-cvintl-custNoDatTrtJob',WORKFLOW_ID='af3459ee68214245a4eed6ecae26d5ea', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvintl-custNoDatTrtJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 20, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('af3459ee68214245a4eed6ecae26d5ea')

    custNoDatTrtJob_vol = []
    custNoDatTrtJob_volMnt = []
    custNoDatTrtJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    custNoDatTrtJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    custNoDatTrtJob_env = [getICISConfigMap('icis-rater-batch-cvintl-configmap'), getICISConfigMap('icis-rater-batch-cvintl-configmap2'), getICISSecret('icis-rater-batch-cvintl-secret')]
    custNoDatTrtJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    custNoDatTrtJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    custNoDatTrtJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '41c36d6fe40a433390a837d81c4a42b6',
        'volumes': custNoDatTrtJob_vol,
        'volume_mounts': custNoDatTrtJob_volMnt,
        'env_from':custNoDatTrtJob_env,
        'task_id':'custNoDatTrtJob',
        'image':'/icis/icis-rater-batch-cvintl:0.4.0.9',
        'arguments':["--job.names=custNoDatTrtJob", "cyclYmd=20240320"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('af3459ee68214245a4eed6ecae26d5ea')

    workflow = COMMON.getICISPipeline([
        authCheck,
        custNoDatTrtJob,
        Complete
    ]) 

    # authCheck >> custNoDatTrtJob >> Complete
    workflow








