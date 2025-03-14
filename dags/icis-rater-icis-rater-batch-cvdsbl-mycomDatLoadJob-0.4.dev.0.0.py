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
                , WORKFLOW_NAME='icis-rater-batch-cvdsbl-mycomDatLoadJob',WORKFLOW_ID='9d774f1d93e148bb9536ea855a942796', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-icis-rater-batch-cvdsbl-mycomDatLoadJob-0.4.dev.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 9, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('9d774f1d93e148bb9536ea855a942796')

    mycomDatLoadJob_vol = []
    mycomDatLoadJob_volMnt = []
    mycomDatLoadJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    mycomDatLoadJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data/'))

    mycomDatLoadJob_env = [getICISConfigMap('icis-rater-batch-cvdsbl-configmap'), getICISConfigMap('icis-rater-batch-cvdsbl-configmap2'), getICISSecret('icis-rater-batch-cvdsbl-secret')]
    mycomDatLoadJob_env.extend([getICISConfigMap('icis-rater-batch-cmmn-configmap'), getICISSecret('icis-rater-batch-cmmn-secret')])
    mycomDatLoadJob_env.extend([getICISConfigMap('icis-rater-batch-truststore.jks')])

    mycomDatLoadJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '91464d7509e24ead8874766db8972921',
        'volumes': mycomDatLoadJob_vol,
        'volume_mounts': mycomDatLoadJob_volMnt,
        'env_from':mycomDatLoadJob_env,
        'task_id':'mycomDatLoadJob',
        'image':'/icis/icis-rater-batch-cvdsbl:0.4.0.7',
        'arguments':['--job.names=mycomDatLoadJob' , 'cyclYm=202407'],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('9d774f1d93e148bb9536ea855a942796')

    workflow = COMMON.getICISPipeline([
        authCheck,
        mycomDatLoadJob,
        Complete
    ]) 

    # authCheck >> mycomDatLoadJob >> Complete
    workflow








