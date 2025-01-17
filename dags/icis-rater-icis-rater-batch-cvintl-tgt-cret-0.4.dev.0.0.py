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
                , WORKFLOW_NAME='icis-rater-batch-cvintl-tgt-cret',WORKFLOW_ID='f8711e0107ac43ff84653b17aa8692e7', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvintl-tgt-cret-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 21, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f8711e0107ac43ff84653b17aa8692e7')

    cvIntlTgtCretJob_vol = []
    cvIntlTgtCretJob_volMnt = []
    cvIntlTgtCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    cvIntlTgtCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    cvIntlTgtCretJob_env = [getICISConfigMap('icis-rater-batch-cvintl-configmap'), getICISConfigMap('icis-rater-batch-cvintl-configmap2'), getICISSecret('icis-rater-batch-cvintl-secret')]
    cvIntlTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    cvIntlTgtCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    cvIntlTgtCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a29b340116f4d3cb32f47ef1e9db159',
        'volumes': cvIntlTgtCretJob_vol,
        'volume_mounts': cvIntlTgtCretJob_volMnt,
        'env_from':cvIntlTgtCretJob_env,
        'task_id':'cvIntlTgtCretJob',
        'image':'/icis/icis-rater-batch-cvintl:0.4.0.4',
        'arguments':["--job.names=cvIntlTgtCretJob", "cyclYm=202403" ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f8711e0107ac43ff84653b17aa8692e7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cvIntlTgtCretJob,
        Complete
    ]) 

    # authCheck >> cvIntlTgtCretJob >> Complete
    workflow








