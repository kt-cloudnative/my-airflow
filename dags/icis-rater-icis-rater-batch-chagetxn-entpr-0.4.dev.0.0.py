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
                , WORKFLOW_NAME='icis-rater-batch-chagetxn-entpr',WORKFLOW_ID='a703d918036f43f080ffec844e317dc4', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-chagetxn-entpr-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 30, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a703d918036f43f080ffec844e317dc4')

    entprChageJob_vol = []
    entprChageJob_volMnt = []
    entprChageJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    entprChageJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    entprChageJob_env = [getICISConfigMap('icis-rater-batch-chagetxn-configmap'), getICISConfigMap('icis-rater-batch-chagetxn-configmap2'), getICISSecret('icis-rater-batch-chagetxn-secret')]
    entprChageJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    entprChageJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    entprChageJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a8c5791038294a1aa01aac21d7435e6f',
        'volumes': entprChageJob_vol,
        'volume_mounts': entprChageJob_volMnt,
        'env_from':entprChageJob_env,
        'task_id':'entprChageJob',
        'image':'/icis/icis-rater-batch-chagetxn:20240731131540',
        'arguments':["--job.names=entprChageJob", "runType=T", "rcpYmd=20240413", "wrkType=M"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a703d918036f43f080ffec844e317dc4')

    authCheck >> entprChageJob >> Complete
    








