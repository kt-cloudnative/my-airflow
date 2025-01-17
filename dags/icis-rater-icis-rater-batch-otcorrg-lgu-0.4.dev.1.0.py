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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg-lgu',WORKFLOW_ID='44a696f354494b20a34753529e76aba9', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg-lgu-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 21, 3, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': True
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('44a696f354494b20a34753529e76aba9')

    lgBfacSelJob_vol = []
    lgBfacSelJob_volMnt = []
    lgBfacSelJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    lgBfacSelJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    lgBfacSelJob_env = [getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISConfigMap('icis-rater-batch-otcorrg-configmap2'), getICISSecret('icis-rater-batch-otcorrg-secret')]
    lgBfacSelJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    lgBfacSelJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    lgBfacSelJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e5a2e16d7d59488fb23ba1b779fecb1c',
        'volumes': lgBfacSelJob_vol,
        'volume_mounts': lgBfacSelJob_volMnt,
        'env_from':lgBfacSelJob_env,
        'task_id':'lgBfacSelJob',
        'image':'/icis/icis-rater-batch-otcorrg:20240723155411',
        'arguments':["--job.names=lgBfacSelJob", "runType=T", "cyclYm=202403"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('44a696f354494b20a34753529e76aba9')

    authCheck >> lgBfacSelJob >> Complete
    








