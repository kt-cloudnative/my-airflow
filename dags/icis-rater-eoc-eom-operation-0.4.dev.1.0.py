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
                , WORKFLOW_NAME='eoc-eom-operation',WORKFLOW_ID='e22c03cc91ae48adbc6b2effed42f4ab', APP_NAME='NBSS_TRAT', CHNL_TYPE='TR', USER_ID='91337910')

with COMMON.getICISDAG({
    'dag_id':'icis-rater-eoc-eom-operation-0.4.dev.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 9, 19, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e22c03cc91ae48adbc6b2effed42f4ab')

    eocSubJob_vol = []
    eocSubJob_volMnt = []
    eocSubJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eocSubJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eocSubJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    eocSubJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    eocSubJob_env = [getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap2'), getICISSecret('icis-rater-engine-eoc-batch-secret')]
    eocSubJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eocSubJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eocSubJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '22f505bd693b434492fa7af60d206ae7',
        'volumes': eocSubJob_vol,
        'volume_mounts': eocSubJob_volMnt,
        'env_from':eocSubJob_env,
        'task_id':'eocSubJob',
        'image':'/icis/icis-rater-engine-eoc-batch:20240919180746',
        'arguments':["--job.names=eocSubJob", "cyclYy=${YYYY,DD,-1}", "cyclMonth=${MM,DD,-1}", "run.id=${YYYYMMDDHHMISS}", "--HIKARI-POOL-SIZE=10"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    eocMainJob_vol = []
    eocMainJob_volMnt = []
    eocMainJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eocMainJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eocMainJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    eocMainJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    eocMainJob_env = [getICISConfigMap('icis-rater-engine-eoc-batch-configmap'), getICISConfigMap('icis-rater-engine-eoc-batch-configmap2'), getICISSecret('icis-rater-engine-eoc-batch-secret')]
    eocMainJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eocMainJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eocMainJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd1630032a19a4cacacb0f20ca18986aa',
        'volumes': eocMainJob_vol,
        'volume_mounts': eocMainJob_volMnt,
        'env_from':eocMainJob_env,
        'task_id':'eocMainJob',
        'image':'/icis/icis-rater-engine-eoc-batch:20240919180746',
        'arguments':["--job.names=eocSubJob", "cyclYy=${YYYY,DD,-1}", "cyclMonth=${MM,DD,-1}", "run.id=${YYYYMMDDHHMISS}", "--HIKARI-POOL-SIZE=10"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    eomMainJob_vol = []
    eomMainJob_volMnt = []
    eomMainJob_vol.append(getVolume('t-rater-ap-pvc','t-rater-ap-pvc'))
    eomMainJob_volMnt.append(getVolumeMount('t-rater-ap-pvc','/rater_data'))

    eomMainJob_vol.append(getVolume('t-rater-bat-pvc','t-rater-bat-pvc'))
    eomMainJob_volMnt.append(getVolumeMount('t-rater-bat-pvc','/batch_data'))

    eomMainJob_env = [getICISConfigMap('icis-rater-engine-eom-batch-configmap'), getICISConfigMap('icis-rater-engine-eom-batch-configmap2'), getICISSecret('icis-rater-engine-eom-batch-secret')]
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-cmmn-configmap'), getICISSecret('icis-rater-engine-cmmn-secret')])
    eomMainJob_env.extend([getICISConfigMap('icis-rater-engine-truststore.jks')])

    eomMainJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bbf437d6141441f5ae4978e3d480a294',
        'volumes': eomMainJob_vol,
        'volume_mounts': eomMainJob_volMnt,
        'env_from':eomMainJob_env,
        'task_id':'eomMainJob',
        'image':'/icis/icis-rater-engine-eom-batch:20240919180756',
        'arguments':["--job.names=eomMainJob", "cyclYy="+str((datetime.now(local_tz)+timedelta(1)).year), "cyclMonth="+str((datetime.now(local_tz)+timedelta(1)).strftime("%m")), "run.id="+str(datetime.now(local_tz).strftime("%Y%m%d%H%M%S"))],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    selector = COMMON.getICISSimpleHttpOperator_v1({
        'id' : '3b31ad02f03f4a9f90cb443fc6675118',
        'task_id' : 'selector',
        'method' : 'POST',
        'endpoint' : 'icis-rater-engine-hotbill-server.t-rater.svc/hotbill/random/value',
        'headers' : {
            "Content-Type": "application/json"
        },
        'data' : {},
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('e22c03cc91ae48adbc6b2effed42f4ab')

    def get_prev_status(ti):
        xcom_value = ti.xcom_pull(task_ids='selector' , key='return_value')
        status = xcom_value.get('status')
        if status == '0':
            return "eomMainJob"
        elif status == '1':
            return "eocSubJob"
        else:
            return "Complete"
    
    branchTask01 = BranchPythonOperator(
        task_id="branchTask01",
        python_callable=get_prev_status,
        dag=dag
    )

    authCheck >> selector >> branchTask01 >> [eomMainJob, eocSubJob, Complete]
    eomMainJob >> Complete
    eocSubJob >> eocMainJob >> Complete







