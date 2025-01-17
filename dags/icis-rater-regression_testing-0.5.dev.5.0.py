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
                , WORKFLOW_NAME='regression_testing',WORKFLOW_ID='a5c00b707d0a4b489cf737e4d9065fe2', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-regression_testing-0.5.dev.5.0'
    ,'schedule_interval':'0 1 * * *'
    ,'start_date': datetime(2024, 12, 3, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a5c00b707d0a4b489cf737e4d9065fe2')

    regressionTestingJob_vol = []
    regressionTestingJob_volMnt = []
    regressionTestingJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    regressionTestingJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    regressionTestingJob_env = [getICISConfigMap('icis-rater-engine-regression-testing-configmap'), getICISConfigMap('icis-rater-engine-regression-testing-configmap2'), getICISSecret('icis-rater-engine-regression-testing-secret')]
    regressionTestingJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    regressionTestingJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    regressionTestingJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '81f89f87285c4f1aa1d46add15a0e388',
        'volumes': regressionTestingJob_vol,
        'volume_mounts': regressionTestingJob_volMnt,
        'env_from':regressionTestingJob_env,
        'task_id':'regressionTestingJob',
        'image':'/icis/icis-rater-engine-regression-testing:20241209161933',
        'arguments':["--job.names=testingJob", "run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a5c00b707d0a4b489cf737e4d9065fe2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        regressionTestingJob,
        Complete
    ]) 

    # authCheck >> regressionTestingJob >> Complete
    workflow








