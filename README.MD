# PIPELINE DE ETL EN GOOGLE CLOUD PLATFORM
En este proyecto Crearemos un pipeline de ETL en Google Cloud Platform.
desde nuestro Data Lake (CLOUD STORAGE) a nuestro Data Ware House (Big Query), todo este proceso automatizado con AIRFLOW Y COMPOSER. además de ello tendremos un cluster
de 3 nodos(maquinas virtuales) para la transformación de datos. 
## ADVERTENCIA 
###### ESTE PROYECTO TIENE COSTOS EN GCP
![](https://scontent.flim1-4.fna.fbcdn.net/v/t39.30808-6/320879758_563737028424811_5873048063175280300_n.jpg?_nc_cat=106&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeGb_2l-UwAJEM8-mTSCD-rJWcqsgTRkGqJZyqyBNGQaosVCWazIRwotqxizy2MMURqnLhO5bXDVfRKJ-p3AbuK1&_nc_ohc=BpOP6tuNBXYAX-Z7Io4&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim1-4.fna&oh=00_AfCB7Wt2kL7vFPOVUFqXysImBcKl5t3SIuTsHFFvM-I-2A&oe=63CC1DF8)

Los archivos planos para este proyecto lo tendran en google drive
https://drive.google.com/drive/folders/1ZTIPaZGOzc-TZY3jbxl9KYQ0G5_tvU43?usp=sharing

## COMENCEMOS
1-Creamos un proyecto en cloud composer como se muestra en la imagen

![](https://scontent.flim1-4.fna.fbcdn.net/v/t39.30808-6/320675445_610255837442600_2780669897925596587_n.jpg?_nc_cat=104&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeHFmOGz6q88h1cbqn8Np8RnbXet2yiCz1xtd63bKILPXNyaeLgZRbUKhv1Ww0HH5dJ0IvmVvyKJVjMV_3OCNyOY&_nc_ohc=-nCmIBg2k40AX_YkRvK&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim1-4.fna&oh=00_AfDSqUf8W4dAzvdSmvSHGXMXtyqL6NfJUy_YioZjJ6sUoQ&oe=63CAE84C)

2- Configuramos un cluster con 30 gb de memoria "es el minimo que permite gcp", 3 nodos "minimo", tipo de maquina N1  y le damos en crear
 Esto tarde aproximadamente 30 min en levantar la infraestructura.
 ![](https://scontent.flim1-4.fna.fbcdn.net/v/t39.30808-6/320661127_687085216403094_1379687921305792831_n.jpg?_nc_cat=100&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeFUzLJhzG55-WxF6wRBKk5gR3NA5aWZmAJHc0DlpZmYAlqktpLpVb1-u_OjSmYTM3nASoMfXUMKxwwVyQvUriQP&_nc_ohc=jCZahWFxj1EAX_3Oq8z&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim1-4.fna&oh=00_AfBvWssvITfvdf012qFJa0o2fotOuc9z9WKUjAqdeTNjnw&oe=63CB8DFB)

![](https://scontent.flim1-4.fna.fbcdn.net/v/t39.30808-6/320661127_687085216403094_1379687921305792831_n.jpg?_nc_cat=100&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeFUzLJhzG55-WxF6wRBKk5gR3NA5aWZmAJHc0DlpZmYAlqktpLpVb1-u_OjSmYTM3nASoMfXUMKxwwVyQvUriQP&_nc_ohc=jCZahWFxj1EAX_3Oq8z&tn=zRTkmcXK30-BI3TQ&_nc_zt=23&_nc_ht=scontent.flim1-4.fna&oh=00_AfBvWssvITfvdf012qFJa0o2fotOuc9z9WKUjAqdeTNjnw&oe=63CB8DFB)

![](https://scontent.flim1-3.fna.fbcdn.net/v/t39.30808-6/320682061_874255880431963_7202749146638401161_n.jpg?_nc_cat=107&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeEDaJlh6hoK3VY5k-z7z8oDE1989N5p6WkTX3z03mnpabRJ-sJY6AhL7lqMGlG2hk9KgQm8SkPV95tZa_ddxbl4&_nc_ohc=k34QOoEXZ-sAX-LL1LV&_nc_zt=23&_nc_ht=scontent.flim1-3.fna&oh=00_AfA9f8hEsLL_s85TAxtESttT7sDNltpJTwsKQwYwUx8emw&oe=63CAEF60)

3 - Agregamos una dependencia al cluster, este nos permitirá manipular archivos excel
![](https://scontent.flim1-4.fna.fbcdn.net/v/t39.30808-6/320884714_5887315704667898_9146768279147815981_n.jpg?_nc_cat=102&ccb=1-7&_nc_sid=730e14&_nc_eui2=AeEyQ_G-PfuKpk-0ljqXRMdycpC_FyroPSlykL8XKug9KYTzVMTe6HaoVDexgkUW7u7wqnU6_tipMH03TZaTQwJZ&_nc_ohc=opMClSCY8E8AX-5r_to&_nc_zt=23&_nc_ht=scontent.flim1-4.fna&oh=00_AfCft7WuuA6o8vPEw6YJrdHQ6biAwLeqMgOD98K76MaYkw&oe=63CB9A96)

4 - 




