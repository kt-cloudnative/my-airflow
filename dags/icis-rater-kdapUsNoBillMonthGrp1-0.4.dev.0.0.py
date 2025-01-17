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
                , WORKFLOW_NAME='kdapUsNoBillMonthGrp1',WORKFLOW_ID='0842f3106139431d9361c8c5f8a30011', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-kdapUsNoBillMonthGrp1-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 12, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0842f3106139431d9361c8c5f8a30011')

    kdapUsNoBillJob_vol = []
    kdapUsNoBillJob_volMnt = []
    kdapUsNoBillJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    kdapUsNoBillJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    kdapUsNoBillJob_env = [getICISConfigMap('icis-rater-batch-kdap-configmap'), getICISConfigMap('icis-rater-batch-kdap-configmap2'), getICISSecret('icis-rater-batch-kdap-secret')]
    kdapUsNoBillJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    kdapUsNoBillJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    kdapUsNoBillJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a4e8cf5d512b4607ba48d313e6d8f2ca',
        'volumes': kdapUsNoBillJob_vol,
        'volume_mounts': kdapUsNoBillJob_volMnt,
        'env_from':kdapUsNoBillJob_env,
        'task_id':'kdapUsNoBillJob',
        'image':'/icis/icis-rater-batch-kdap:0.4.0.16',
        'arguments':["--job.names=kdapUsNoBillJob", "retvType=M", "cyclYm=202407",  "cyclGroup=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0842f3106139431d9361c8c5f8a30011')

    workflow = COMMON.getICISPipeline([
        authCheck,
        kdapUsNoBillJob,
        Complete
    ]) 

    # authCheck >> kdapUsNoBillJob >> Complete
    workflow








