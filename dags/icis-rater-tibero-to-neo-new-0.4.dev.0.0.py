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
                , WORKFLOW_NAME='tibero-to-neo-new',WORKFLOW_ID='f349a24170014333801ced6b2e93527d', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-tibero-to-neo-new-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 23, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f349a24170014333801ced6b2e93527d')

    task003_vol = []
    task003_volMnt = []
    task003_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    task003_volMnt.append(getVolumeMount('t-rater-ap-pvc','/batch_data/'))

    task003_env = [getICISConfigMap('icis-rater-uq-tibero-to-neo-monthy-configmap'), getICISConfigMap('icis-rater-uq-tibero-to-neo-monthy-configmap2'), getICISSecret('icis-rater-uq-tibero-to-neo-monthy-secret')]
    task003_env.extend([getICISConfigMap('icis-rater-uq-cmmn-configmap'), getICISSecret('icis-rater-uq-cmmn-secret')])
    task003_env.extend([getICISConfigMap('icis-rater-uq-truststore.jks')])

    task003 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b69c837b65714143be376970fe5df704',
        'volumes': task003_vol,
        'volume_mounts': task003_volMnt,
        'env_from':task003_env,
        'task_id':'task003',
        'image':'/icis/icis-rater-uq-tibero-to-neo-monthy:0.4.0.13',
        'arguments':["--job.names=tiberoToNeoJob", "retvYm=202403", "step=3"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f349a24170014333801ced6b2e93527d')

    authCheck >> task003 >> Complete
    








