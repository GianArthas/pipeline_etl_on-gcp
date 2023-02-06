# PIPELINE DE ETL EN GOOGLE CLOUD PLATFORM
En este proyecto Crearemos un pipeline de ETL en Google Cloud Platform.
desde nuestro Data Lake (CLOUD STORAGE) a nuestro Data Ware House (Big Query), todo este proceso automatizado con AIRFLOW Y COMPOSER. además de ello tendremos un cluster
de 3 nodos(maquinas virtuales) para tener mas poder de procesamiento en transformación de datos. 
## ADVERTENCIA 
###### ESTE PROYECTO TIENE COSTOS EN GCP
![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/pipeline%20etl%20en%20gcp.jpeg?raw=true)

Los archivos planos para este proyecto lo tendran en google drive
https://drive.google.com/drive/folders/1ZTIPaZGOzc-TZY3jbxl9KYQ0G5_tvU43?usp=sharing

## COMENCEMOS
1-Creamos un proyecto en cloud composer como se muestra en la imagen

![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/pagina%20compositor.JPG?raw=true)

2- Configuramos un cluster con 30 gb de memoria "es el minimo que permite gcp", 3 nodos "minimo", tipo de maquina N1  y le damos en crear.
 Esto tarda aproximadamente 30 min en levantar la infraestructura.
 ![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/configuracion%20cluster.JPG?raw=true)

![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/configuracion%20cluster%202.JPG?raw=true)

![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/30%20min%20aprox.JPG?raw=true)

3 - Agregamos una dependencia al cluster, este nos permitirá manipular archivos excel
![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/dependencias.JPG?raw=true)

4 - Nos movemos al bucket de nuestro proyecto donde veremos una carpeta DATA y otra DAGS
    en DATA almacenaremos nuestros datos para que sean transformado y en DAGS subiremos nuestro script de python.
 
![](https://github.com/GianArthas/pipeline_etl_on-gcp/blob/main/capturas/cloud%20storage%20data.JPG?raw=true)
5 - continuara...



