from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import json
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 12),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
}

def parse_response(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids='fetchRepo')

    if response:
        try:
            response_data = json.loads(response)
            num_repos = response_data.get('public_repos', 0)
            print(f"Você possui {num_repos} repositórios")
            return num_repos
        except json.JSONDecodeError as e:
            print(f"Erro ao converter resposta para JSON: {str(e)}")
            return None
    else:
        print("Não foi possível obter a resposta.")
        return None

def check30(**kwargs):
    ti = kwargs['ti']
    num_repos = ti.xcom_pull(task_ids='parse_repos')

    if num_repos is not None:
        if num_repos > 30:
            print("Número de repositórios é maior do que 30.")
        elif num_repos == 30:
            print("Número de repositórios é igual a 30.")
        else:
            print("Número de repositórios é menor do que 30.")
    else:
        print("Não foi possível obter o número de repositórios.")

with DAG('github_respos_count', 
        default_args=default_args, 
        schedule_interval='30 * * * *') as dag:

    fetchRepos = SimpleHttpOperator(
        task_id='fetchRepo',
        method='GET',
        http_conn_id='github_api',
        endpoint='user',
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    parse_repos = PythonOperator(
        task_id='parse_repos',
        python_callable=parse_response,
        provide_context=True,
    )

    check30 = PythonOperator(
        task_id='check30',
        python_callable=check30,
        provide_context=True,
    )

fetchRepos >> parse_repos >> check30
