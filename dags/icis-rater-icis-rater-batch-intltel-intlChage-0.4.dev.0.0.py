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
                , WORKFLOW_NAME='icis-rater-batch-intltel-intlChage',WORKFLOW_ID='bdb811c11d3941f6928d642de52c7367', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-intltel-intlChage-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bdb811c11d3941f6928d642de52c7367')

    intlChageJob_vol = []
    intlChageJob_volMnt = []
    intlChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    intlChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    intlChageJob_env = [getICISConfigMap('icis-rater-batch-intltel-configmap'), getICISConfigMap('icis-rater-batch-intltel-configmap2'), getICISSecret('icis-rater-batch-intltel-secret')]
    intlChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    intlChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    intlChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '031006d9c17f4d6283653ec22bc70b56',
        'volumes': intlChageJob_vol,
        'volume_mounts': intlChageJob_volMnt,
        'env_from':intlChageJob_env,
        'task_id':'intlChageJob',
        'image':'/icis/icis-rater-batch-intltel:20240902170728',
        'arguments':["--job.names=intlChageJob", "cyclYm=202403", "cyclYmd=20240301"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bdb811c11d3941f6928d642de52c7367')

    workflow = COMMON.getICISPipeline([
        authCheck,
        intlChageJob,
        Complete
    ]) 

    # authCheck >> intlChageJob >> Complete
    workflow








