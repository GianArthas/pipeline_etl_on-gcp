import pandas as pd
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    'owner': 'gian_rivas',
}


#ruta de datos de entrada
precios_518_input_path = "/home/airflow/gcs/data/precios_semana_20200518.txt"


#ruta de datos de archivos de salida despues de la transformacion
precios_518_out_path='/home/airflow/gcs/data/data_transformada/precios_518.csv'

def transform_data_txt(precios_518_input_path, precios_518_out_path):
    precios_518 = pd.read_csv(precios_518_input_path,sep = "|")
    precios_518['sucursal_id'] = precios_518['sucursal_id'].str.strip() #quitando espacios en blanco
    precios_518['precio'] = precios_518['precio'].astype(float) # cambiando el tipo de dato a flotante
    precios_518 = precios_518.reindex(columns=['sucursal_id','producto_id','precio'])
    precios_518.to_csv(precios_518_out_path, index=False)

with DAG(
    'carga_incremental',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['carga','incremental']
) as dag:
    dag.doc_md = "carga_incremental_precios_518"

    t1 = PythonOperator(
        task_id='transform_data_txt',
        python_callable=transform_data_txt,
        op_kwargs={'precios_518_input_path': precios_518_input_path, 'precios_518_out_path':precios_518_out_path},
    )

    t2 = GCSToBigQueryOperator(
        task_id='CargandoDatosToBigquery',
        bucket='us-central1-demo-2-be3cf527-bucket',
        source_objects=['data/data_transformada/precios_518.csv'],
        field_delimiter =',',
        destination_project_dataset_table='pi_soy_henry.precios',
        skip_leading_rows=1,
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'sucursal_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'producto_id',
                'type': 'STRING'
            },
                        {
                'mode': 'NULLABLE',
                'name': 'precio',
                'type': 'FLOAT'
            },
        ],
        write_disposition='WRITE_APPEND',
    )
    t1 >> t2