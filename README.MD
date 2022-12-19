# PIPELINE DE ETL EN GOOGLE CLOUD PLATFORM
En este proyecto Crearemos un pipeline de ETL en Google Cloud Platform.
desde nuestro Data Lake (CLOUD STORAGE) a nuestro Data Ware House (Big Query), todo este proceso automatizado con AIRFLOW Y COMPOSER. además de ello tendremos un cluster
de 3 nodos(maquinas virtuales) para la transformación de datos. 
## ADVERTENCIA 
###### ESTE PROYECTO TIENE COSTOS EN GCP
![](https://scontent.flim2-2.fna.fbcdn.net/v/t39.30808-6/320879758_563737028424811_5873048063175280300_n.jpg?_nc_cat=106&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeGb_2l-UwAJEM8-mTSCD-rJWcqsgTRkGqJZyqyBNGQaosVCWazIRwotqxizy2MMURqnLhO5bXDVfRKJ-p3AbuK1&_nc_ohc=ahooAIAE4mkAX8iGKak&_nc_zt=23&_nc_ht=scontent.flim2-2.fna&oh=00_AfABwc9LInf2QRbvkPVJMGE1DUu3T5wcXEKdKfBuGknviQ&oe=63A490F8)

Los archivos planos para este proyecto lo tendran en google drive
https://drive.google.com/drive/folders/1ZTIPaZGOzc-TZY3jbxl9KYQ0G5_tvU43?usp=sharing

## COMENCEMOS
1-Creamos un proyecto en cloud composer como se muestra en la imagen

![](https://scontent.flim2-3.fna.fbcdn.net/v/t39.30808-6/320675445_610255837442600_2780669897925596587_n.jpg?_nc_cat=104&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeHmR-xiuhlFuZcBWtlUeCTnbXet2yiCz1xtd63bKILPXCs52NrVjkQcWdDkNcxYzx64BqTrzNjqBghISFaLT1I2&_nc_ohc=ouX-2grCAFAAX_3FO8V&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim2-3.fna&oh=00_AfAqvbH0lklM01nfahnhx1TPEbNnWwAszDhtawgsXaw-iw&oe=63A5558C)

2- Configuramos un cluster con 30 gb de memoria "es el minimo que permite gcp", 3 nodos "minimo", tipo de maquina N1  y le damos en crear
 Esto tarde aproximadamente 30 min en levantar la infraestructura.
 ![]()





