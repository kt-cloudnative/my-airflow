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
                , WORKFLOW_NAME='regression_verifying',WORKFLOW_ID='d6999c73890c44a2975d83f57bdcab04', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-regression_verifying-0.5.dev.0.0'
    ,'schedule_interval':'0 2 * * *'
    ,'start_date': datetime(2024, 12, 3, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d6999c73890c44a2975d83f57bdcab04')

    regressionVerifyingJob_vol = []
    regressionVerifyingJob_volMnt = []
    regressionVerifyingJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    regressionVerifyingJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    regressionVerifyingJob_env = [getICISConfigMap('icis-rater-engine-regression-testing-configmap'), getICISConfigMap('icis-rater-engine-regression-testing-configmap2'), getICISSecret('icis-rater-engine-regression-testing-secret')]
    regressionVerifyingJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    regressionVerifyingJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    regressionVerifyingJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '02a9f669dade4c68953359fe51ab6f60',
        'volumes': regressionVerifyingJob_vol,
        'volume_mounts': regressionVerifyingJob_volMnt,
        'env_from':regressionVerifyingJob_env,
        'task_id':'regressionVerifyingJob',
        'image':'/icis/icis-rater-engine-regression-testing:20241203091422',
        'arguments':["--job.names=verifyingJob", "cdrFileFmtId=L7", "fileId=4264243", "run.id=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d6999c73890c44a2975d83f57bdcab04')

    workflow = COMMON.getICISPipeline([
        authCheck,
        regressionVerifyingJob,
        Complete
    ]) 

    # authCheck >> regressionVerifyingJob >> Complete
    workflow








