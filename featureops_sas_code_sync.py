from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.models import Variable
from git import Repo
import os
import subprocess


with DAG(
    'sas_code_sync',
    description='Sync SAS Code DAG',
    schedule='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['featureops'],
) as dag:

    sas_dir = "/opt/airflow/dags/sas_python_code"
    git_token=Variable.get('git_token')
    repo_url=f"https://jiangxuemichelle:{git_token}@github.com/otp-featureops/sas_python_code.git"
    
    def clone_repo():
        if os.path.isdir(sas_dir) and os.path.isdir(os.path.join(sas_dir,'.git')):
            return None
        else:
           print(repo_url)
           subprocess.run(['git', 'clone', repo_url, sas_dir])
           return True



    def create_new_branch():
        repo = Repo(sas_dir)
        current_branch = repo.active_branch.name
        repo.remotes.origin.pull()
        if repo.is_dirty(untracked_files=True):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            new_branch_name = f'code_changes_{timestamp}'
            new_branch = repo.create_head(new_branch_name)
            new_branch.checkout()
            return new_branch_name
        else:
            return None

    def convert_sas_code():
        print("Converting sas code to python...")



    clone_code = PythonOperator(
        task_id='clone_code',
        python_callable=clone_repo,
    )

    branch_check = BranchPythonOperator(
        task_id='branch_check',
        python_callable=lambda: 'convert_sas_code' if create_new_branch() else 'no_changes',
    )

    convert_sas_code = PythonOperator(
        task_id='convert_sas_code',
        python_callable=lambda:convert_sas_code()
    )

    no_changes = PythonOperator(
        task_id='no_changes',
        python_callable=lambda: print("No new branch created.."),
    )



    clone_code >> branch_check >> [convert_sas_code, no_changes]



