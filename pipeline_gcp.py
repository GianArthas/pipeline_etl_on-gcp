import pandas as pd
import requests,os
import json
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    #BigQueryCreateExternalTableOperator,
    #BigQueryDeleteDatasetOperator,
    #BigQueryDeleteTableOperator,
    #BigQueryGetDatasetOperator,
    #BigQueryGetDatasetTablesOperator,
    #BigQueryPatchDatasetOperator,
    #BigQueryUpdateDatasetOperator,
    #BigQueryUpdateTableOperator,
    #BigQueryUpsertTableOperator,
)
#from airflow.operators.email_operator import EmailOperator


default_args = {
    'owner': 'gian_rivas',
}

#ruta de archivos de  datos de entrada
precios_413_input_path = "/home/airflow/gcs/data/precios_semana_20200413.csv"
precios_419_426_input_path = "/home/airflow/gcs/data/precios_semanas_20200419_20200426.xlsx"
precios_503_input_path = '/home/airflow/gcs/data/precios_semana_20200503.json'
producto_input_path = '/home/airflow/gcs/data/producto.parquet'
sucursal_input_path = '/home/airflow/gcs/data/sucursal.csv'



#ruta de datos de archivos de salida despues de la transformacion
precios_413_out_path='/home/airflow/gcs/data/data_transformada/precios_413.csv'
precios_419_out_path='/home/airflow/gcs/data/data_transformada/precios_419.csv'
precios_426_out_path='/home/airflow/gcs/data/data_transformada/precios_426.csv'
precios_503_out_path = '/home/airflow/gcs/data/data_transformada/precios_503.csv'
producto_out_path = '/home/airflow/gcs/data/data_transformada/producto.csv'
sucursal_out_path = '/home/airflow/gcs/data/data_transformada/sucursal.csv'

#transform_data_output_path = '/home/airflow/gcs/data/transform_data.csv'
def create_carpeta():
    if  not os.path.exists('/home/airflow/gcs/data/data_transformada'):
        os.mkdir('/home/airflow/gcs/data/data_transformada')

def transform_data_excel(precios_419_426_input_path, precios_419_out_path, precios_426_out_path):
    precios_419 = pd.read_excel(precios_419_426_input_path, sheet_name='precios_20200419_20200419',engine='openpyxl')
    precios_426 = pd.read_excel(precios_419_426_input_path, sheet_name='precios_20200426_20200426',engine='openpyxl')
    precios_419['sucursal_id'] = precios_419['sucursal_id'].str.strip() #quitando espacios en blanco
    precios_419['precio'] = precios_419['precio'].astype(float) # cambiando el tipo de dato a flotante
    precios_426['sucursal_id'] = precios_426['sucursal_id'].str.strip() #quitando espacios en blanco
    precios_426['precio'] = precios_426['precio'].astype(float) #quitando espacios en blanco
    precios_419 = precios_419.reindex(columns=['sucursal_id','producto_id','precio'])
    precios_426 = precios_426.reindex(columns=['sucursal_id','producto_id','precio'])
    precios_419.to_csv(precios_419_out_path, index=False)
    precios_426.to_csv(precios_426_out_path, index=False)


def transform_data_csv(precios_413_input_path, precios_413_out_path):
    precios_413 = pd.read_csv(precios_413_input_path,encoding = 'utf-16')
    precios_413['sucursal_id'] = precios_413['sucursal_id'].str.strip() #quitando espacios en blanco
    precios_413['precio'] = precios_413['precio'].astype(float) # cambiando el tipo de dato a flotante
    precios_413 = precios_413.reindex(columns=['sucursal_id','producto_id','precio'])
    precios_413.to_csv(precios_413_out_path, index=False)
    

def transform_data_json(precios_503_input_path, precios_503_out_path):
    precios_503 = pd.read_json(precios_503_input_path)
    precios_503['sucursal_id'] = precios_503['sucursal_id'].str.strip() #quitando espacios en blanco
    precios_503['precio'] = precios_503['precio'].astype(str) # cambiando el tipo de dato a flotante
    precios_503['precio'] = precios_503['precio'].replace('', 'nada') #sucedian cosas raras al hacerlo directo
    precios_503['precio'] = precios_503['precio'].replace('nada','0').astype(float)
    precios_503 = precios_503.reindex(columns=['sucursal_id','producto_id','precio'])
    precios_503.to_csv(precios_503_out_path, index=False)


def transform_data_producto(producto_input_path,producto_out_path):
    producto = pd.read_parquet(producto_input_path)
    producto = producto.reindex(columns = ['id','marca','nombre','presentacion'])
    producto['id'].astype(str)
    producto.to_csv(producto_out_path,index=False)


def transform_data_sucursal(sucursal_input_path,sucursal_out_path):
    sucursal = pd.read_csv(sucursal_input_path)
    sucursal = sucursal.reindex(columns=['id','provincia','lat','lng','sucursalTipo'])
    sucursal.to_csv( sucursal_out_path,index = False)



with DAG(
    'proyecto_individual_henry',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['etl','proyecto_individual']
) as dag:
    dag.doc_md = "tareas_de_proyecto_individual"

    t0 = PythonOperator(
        task_id='crear_carpeta',
        python_callable=create_carpeta,
    )

    t1 = PythonOperator(
        task_id='transformar_datos_csv',
        python_callable=transform_data_csv,
        op_kwargs={'precios_413_input_path': precios_413_input_path, 'precios_413_out_path':precios_413_out_path},
    )

    t2 = PythonOperator(
        task_id='transformar_datos_excel',
        python_callable=transform_data_excel,
        op_kwargs={'precios_419_426_input_path': precios_419_426_input_path, 'precios_419_out_path':precios_419_out_path,'precios_426_out_path':precios_426_out_path},
    )

    t3 = PythonOperator(
        task_id = 'transform_data_json',
        python_callable = transform_data_json,
        op_kwargs ={'precios_503_input_path':precios_503_input_path,'precios_503_out_path':precios_503_out_path}
    )

    t4 = PythonOperator(
        task_id = 'transform_data_producto',
        python_callable = transform_data_producto,
        op_kwargs ={'producto_input_path':producto_input_path,'producto_out_path':producto_out_path}
    )
    t5 = PythonOperator(
        task_id = 'transform_data_sucursal',
        python_callable = transform_data_sucursal,
        op_kwargs ={'sucursal_input_path':sucursal_input_path,'sucursal_out_path':sucursal_out_path}
    )


    t6 = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", 
        dataset_id='pi_soy_henry'
    )


    t7 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_precios",
        dataset_id='pi_soy_henry',
        table_id="precios",
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
    )

    t8 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_producto",
        dataset_id='pi_soy_henry',
        table_id="producto",
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'producto_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'marca',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'nombre',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'presentacion',
                'type': 'STRING'
            },
        ],
    )

    t9 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_sucursal",
        dataset_id='pi_soy_henry',
        table_id="sucursal",
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'sucursal_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'provincia',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'latitud',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'longitud',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'sucursalTipo',
                'type': 'STRING'
            },
        ],
    )


    t10 = GCSToBigQueryOperator(
        task_id='CargandoDatosToBigquery',
        bucket='us-central1-demo-2-be3cf527-bucket', 
        source_objects=['data/data_transformada/precios_413.csv','data/data_transformada/precios_419.csv',
                        'data/data_transformada/precios_426.csv','data/data_transformada/precios_503.csv'],
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

    t11 = GCSToBigQueryOperator(
        task_id='CargandoProductosToBigquery',
        bucket='us-central1-demo-2-be3cf527-bucket',
        source_objects=['data/data_transformada/producto.csv'],
        field_delimiter =',',
        destination_project_dataset_table='pi_soy_henry.producto',
        skip_leading_rows=1,
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'producto_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'marca',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'nombre',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'presentacion',
                'type': 'STRING'
            },
        ],
        write_disposition='WRITE_APPEND',
    )

    t12 = GCSToBigQueryOperator(
        task_id='CargandoSucursalToBigquery',
        bucket='us-central1-demo-2-be3cf527-bucket',
        source_objects=['data/data_transformada/sucursal.csv'],
        field_delimiter =',',
        destination_project_dataset_table='pi_soy_henry.sucursal',
        skip_leading_rows=1,
        schema_fields=[
            {
                'mode': 'NULLABLE',
                'name': 'sucursal_id',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'provincia',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'latitud',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'longitud',
                'type': 'STRING'
            },
            {
                'mode': 'NULLABLE',
                'name': 'sucursalTipo',
                'type': 'STRING'
            },
        ],
        write_disposition='WRITE_APPEND',
    )
    t0 >> t1
    t0 >> t2
    t0 >> t3
    t0 >> t4 
    t0 >> t5

    t1 >> t6
    t2 >> t6
    t3 >> t6
    t4 >> t6
    t5 >> t6

    t6 >> t7
    t6 >> t8
    t6 >> t9

    t7 >> t10
    t8 >> t10
    t9 >> t10
    t9 >> t11 
    t9 >> t12

#eliminar dataset bq rm -f -d --project_id=ultimate-project22 pi_soy_henry 
