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
                , WORKFLOW_NAME='icis-rater-batch-posdata',WORKFLOW_ID='e8c18046ce09464889fca66425759169', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-posdata-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 6, 9, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e8c18046ce09464889fca66425759169')

    posdataAgnRatJob_vol = []
    posdataAgnRatJob_volMnt = []
    posdataAgnRatJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    posdataAgnRatJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    posdataAgnRatJob_env = [getICISConfigMap('icis-rater-batch-posdata-configmap'), getICISConfigMap('icis-rater-batch-posdata-configmap2'), getICISSecret('icis-rater-batch-posdata-secret')]
    posdataAgnRatJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    posdataAgnRatJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    posdataAgnRatJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '15bab51606ad4fc5bb8ca7e90925fbf2',
        'volumes': posdataAgnRatJob_vol,
        'volume_mounts': posdataAgnRatJob_volMnt,
        'env_from':posdataAgnRatJob_env,
        'task_id':'posdataAgnRatJob',
        'image':'/icis/icis-rater-batch-posdata:20240806095600',
        'arguments':["--job.names=posdataAgnRatJob", "runType=T", "cyclYm=202405"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e8c18046ce09464889fca66425759169')

    authCheck >> posdataAgnRatJob >> Complete
    








