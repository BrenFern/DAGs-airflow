from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import json
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
}

def querymysqlsavejson():
    conn_string = 'mysql://root:SenhaSegura#123@localhost/employees'
    tables = ['titles']  
    
    engine = create_engine(conn_string)
    connection = engine.connect()
    
    for table in tables:
        query = f"SELECT * FROM {table};"
        result = connection.execute(query)
        
  
        rows = [dict(row) for row in result.fetchall()]
        
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        json_filename = f"/home/brenfern/dados_{table}_{timestamp}.json"
        
    
        with open(json_filename, 'w') as json_file:
            json.dump(rows, json_file, indent=4)
        
        print(f"Os dados da tabela {table} foram salvos em {json_filename}")
    
    connection.close()

with DAG(
    dag_id='mysqltojson',
    default_args=default_args,
    description='Exporta dados do MySQL para arquivos JSON',
    schedule_interval=None,
) as dag:

    export_task = PythonOperator(
        task_id='querymysqlsavejson',
        python_callable=querymysqlsavejson,
    )

export_task
