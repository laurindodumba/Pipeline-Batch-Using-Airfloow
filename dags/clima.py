from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import csv
import os


def extrair_clima_csv():
    # Datas
    data_inicial = datetime.utcnow()
    data_fim = data_inicial + timedelta(days=7)

    data_inicial_str = data_inicial.strftime('%Y-%m-%d')
    data_fim_str = data_fim.strftime('%Y-%m-%d')

    # Configurações
    city = 'Boston'
    api_key = Variable.get("VISUAL_CROSSING_API_KEY")

    url = (
        f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/'
        f'timeline/{city}/{data_inicial_str}/{data_fim_str}'
        f'?key={api_key}&unitGroup=metric'
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Caminho do CSV
    output_dir = '/opt/airflow/dags/output'
    os.makedirs(output_dir, exist_ok=True)

    caminho_csv = f'{output_dir}/clima_{city}_{data_inicial_str}.csv'

    with open(caminho_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        writer.writerow([
            'cidade',
            'data',
            'temp_max',
            'temp_min',
            'temp_media',
            'umidade',
            'condicoes'
        ])

        for dia in data['days']:
            writer.writerow([
                city,
                dia.get('datetime'),
                dia.get('tempmax'),
                dia.get('tempmin'),
                dia.get('temp'),
                dia.get('humidity'),
                dia.get('conditions')
            ])

    print(f'CSV gerado com sucesso: {caminho_csv}')
    return caminho_csv


# =====================
# DAG
# =====================
local_tz = pendulum.timezone("America/Sao_Paulo")
with DAG(
    dag_id='clima_visual_crossing_csv',
    start_date=datetime(2024, 1, 1),
    schedule='50 20 * * *',
    catchup=False,
    tags=['clima', 'api', 'csv']
) as dag:

    extrair_clima = PythonOperator(
        task_id='extrair_clima_csv',
        python_callable=extrair_clima_csv
    )
