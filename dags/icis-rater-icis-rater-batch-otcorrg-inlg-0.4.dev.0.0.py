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
                , WORKFLOW_NAME='icis-rater-batch-otcorrg-inlg',WORKFLOW_ID='26ca95e6abde4d57bb9f03347706486f', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-otcorrg-inlg-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 22, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('26ca95e6abde4d57bb9f03347706486f')

    bfacSelOrrgCretJob_vol = []
    bfacSelOrrgCretJob_volMnt = []
    bfacSelOrrgCretJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    bfacSelOrrgCretJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    bfacSelOrrgCretJob_env = [getICISConfigMap('icis-rater-batch-otcorrg-configmap'), getICISConfigMap('icis-rater-batch-otcorrg-configmap2'), getICISSecret('icis-rater-batch-otcorrg-secret')]
    bfacSelOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    bfacSelOrrgCretJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    bfacSelOrrgCretJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ba7ef8257c9f4dadb5f5739647652e53',
        'volumes': bfacSelOrrgCretJob_vol,
        'volume_mounts': bfacSelOrrgCretJob_volMnt,
        'env_from':bfacSelOrrgCretJob_env,
        'task_id':'bfacSelOrrgCretJob',
        'image':'/icis/icis-rater-batch-otcorrg:20241023173709',
        'arguments':["--job.names=bfacSelOrrgCretJob", "cyclYm=202407", "otcomDiv=D"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('26ca95e6abde4d57bb9f03347706486f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        bfacSelOrrgCretJob,
        Complete
    ]) 

    # authCheck >> bfacSelOrrgCretJob >> Complete
    workflow








