from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pandas as pd
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

def querymysqlsavecsv():
    conn_string = 'mysql://root:SenhaSegura#123@localhost/employees'
    tables = ['titles']  
    
    engine = create_engine(conn_string)
    connection = engine.connect() 
    
    for table in tables:
        query = f"SELECT * FROM {table};"
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())  
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S") 
        csv_filename = f"/home/brenfern/dados_{table}_{timestamp}.csv"  
        
        df.to_csv(csv_filename, index=False)
        print(f"Os dados da tabela {table} foram salvos em {csv_filename}")
    
    connection.close() 

with DAG(
    dag_id='mysqltocsv',
    default_args=default_args,
    description='Exporta dados do MySQL para arquivos CSV',
    schedule_interval=None,
) as dag:

    export_task = PythonOperator(
        task_id='querymysqlsavecsv',
        python_callable=querymysqlsavecsv,
    )

export_task
