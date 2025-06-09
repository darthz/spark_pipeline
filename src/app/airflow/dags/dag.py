from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def choose_crawler_branch(execution_date, **kwargs):
    # execution_date é um pendulum datetime
    day = execution_date.day
    if day % 7 == 0:
        return 'run_crawler_ag'
    elif day % 2 == 0:
        return 'run_crawler_jvs'
    else:
        return 'skip_crawler'

def choose_upload_branch(execution_date, **kwargs):
    day = execution_date.day
    if day % 7 == 0:
        return 'upload_gold_ag'
    elif day % 2 == 0:
        return 'upload_gold_jvs'
    else:
        return 'skip_upload_gold'

with DAG(
    dag_id='perdcomp_pipeline',
    default_args=default_args,
    description='Pipeline PERDCOMP com triggers diferenciados para crawler',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['perdcomp', 'etl'],
) as dag:

    start = DummyOperator(task_id='start')

    branch_crawler = BranchPythonOperator(
        task_id='branch_crawler',
        python_callable=choose_crawler_branch,
        provide_context=True,
    )

    run_crawler_jvs = DockerOperator(
        task_id='run_crawler_jvs',
        image='perdcomp_app:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/app/crawler.py --filename cnpjs_jvs.txt",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # ...env vars if needed...
        },
        mount_tmp_dir=False,
        mounts=[
            # Mount volumes if needed
        ],
    )

    run_crawler_ag = DockerOperator(
        task_id='run_crawler_ag',
        image='perdcomp_app:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/app/crawler.py --filename cnpjs_ag.txt",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # ...env vars if needed...
        },
        mount_tmp_dir=False,
        mounts=[
            # Mount volumes if needed
        ],
    )

    skip_crawler = DummyOperator(task_id='skip_crawler')

    # Build pipeline (trusted, refined, enriched, upload) - sempre roda após o branch
    build_pipeline = DockerOperator(
        task_id='build_pipeline',
        image='perdcomp_app:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/app/build.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # ...env vars if needed...
        },
        mount_tmp_dir=False,
        mounts=[
            # Mount volumes if needed
        ],
    )

    # Após build_pipeline, decidir upload
    branch_upload = BranchPythonOperator(
        task_id='branch_upload',
        python_callable=choose_upload_branch,
        provide_context=True,
    )

    upload_gold_jvs = DockerOperator(
        task_id='upload_gold_jvs',
        image='perdcomp_app:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/app/upload.py --table enriched_jvs --mode append",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # ...env vars if needed...
        },
        mount_tmp_dir=False,
        mounts=[
            # Mount volumes if needed
        ],
    )

    upload_gold_ag = DockerOperator(
        task_id='upload_gold_ag',
        image='perdcomp_app:latest',
        api_version='auto',
        auto_remove=True,
        command="python src/app/upload.py --table enriched_ag --mode append",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            # ...env vars if needed...
        },
        mount_tmp_dir=False,
        mounts=[
            # Mount volumes if needed
        ],
    )

    skip_upload_gold = DummyOperator(task_id='skip_upload_gold')

    end = DummyOperator(task_id='end')

    # DAG dependencies
    start >> branch_crawler
    branch_crawler >> [run_crawler_jvs, run_crawler_ag, skip_crawler]
    [run_crawler_jvs, run_crawler_ag, skip_crawler] >> build_pipeline
    build_pipeline >> branch_upload
    branch_upload >> [upload_gold_jvs, upload_gold_ag, skip_upload_gold]
    [upload_gold_jvs, upload_gold_ag, skip_upload_gold] >> end
