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
 ![](https://scontent.flim2-4.fna.fbcdn.net/v/t39.30808-6/320661127_687085216403094_1379687921305792831_n.jpg?_nc_cat=100&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeFydH4JfmX1EvJ5ASF620EoR3NA5aWZmAJHc0DlpZmYApdW9C6kXlK8nPcFxFT_oljXilpamOBIyBo5F2s8A9mw&_nc_ohc=CefLIrRMeeIAX-ya9-5&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim2-4.fna&oh=00_AfBXFo8gT8tE7LdZ2C_qOh9Vy13EL_HpcOrYDB4NO47fnQ&oe=63A400FB)

![](https://scontent.flim2-4.fna.fbcdn.net/v/t39.30808-6/320682061_874255880431963_7202749146638401161_n.jpg?_nc_cat=107&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeGXQFlNsuEYe4LNuIQomeabE1989N5p6WkTX3z03mnpaUhwoUt4TlSy5exE0kHJdb8wBlsXecz3cpjArqV6pc5X&_nc_ohc=YWQ47lx_T_gAX_Osfso&_nc_zt=23&_nc_ht=scontent.flim2-4.fna&oh=00_AfBqCs8qNG6r4LZD4xpwC-Fqnt8wOD91kBeQlBCEB4dayA&oe=63A55CA0)

![](https://scontent.flim2-3.fna.fbcdn.net/v/t39.30808-6/320753806_525234052697883_4772281953659826025_n.jpg?_nc_cat=104&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeFX5KY3Yz2sXq1Db5TX4Ret4hlmmXX22rriGWaZdfbaurZt09eyf04nyGfAPu9et3pDEGERxsbylN6Ax8E_F8sn&_nc_ohc=6aytNIE-mzoAX-rdarR&_nc_zt=23&_nc_ht=scontent.flim2-3.fna&oh=00_AfBa9i7t_Dj_VeJqookkQ519NtT8Rx5YmVL4wSR-zHOo8g&oe=63A3B389)

3 - Agregamos una dependencia al cluster, este nos permitirá manipular archivos excel
![](https://scontent.flim2-2.fna.fbcdn.net/v/t39.30808-6/320884714_5887315704667898_9146768279147815981_n.jpg?_nc_cat=102&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeELd4X55oMb8fbQEvEE8F57cpC_FyroPSlykL8XKug9KdpXdZRdqvR5WsXLj3p7S556TdQsp8iDCCINQ2xHQG9X&_nc_ohc=VXDdGi6w0qgAX9NGJRB&_nc_zt=23&_nc_ht=scontent.flim2-2.fna&oh=00_AfCqHGX22EBsmDDkCMhWOIIOjijdwkMPjcJKv8FUla-9Aw&oe=63A40D96)

4 - 




