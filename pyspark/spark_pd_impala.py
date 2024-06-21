from pyspark.sql import SparkSession

from pyspark import SparkContext

from pyspark import SparkConf

import base64

import os

import pyodbc

import pandas as pd

#######################################

               custom_conf={

                   "master":"local[*]",

                   "spark.executor.memory" : "2g",

                   "spark.executor.cores" : "2",

                   "spark.executor.instances":"4",

                   "spark.yarn.keytab" : "/tmp/user.keytab",

                   "spark.yarn.principal" : "***********",

                   "appName" : "Hello World 3",

                                   "spark.sql.catalogImplementation": "hive"

                      }

 

  sc_conf = SparkConf()

  sc = SparkContext()

 

  sc_conf.setAppName(custom_conf["appName"])

  sc_conf.setMaster(custom_conf["master"])

  sc_conf.set('spark.executor.memory', custom_conf["spark.executor.memory"])

  sc_conf.set('spark.executor.cores', custom_conf["spark.executor.cores"])

  sc_conf.set('spark.yarn.keytab', custom_conf["spark.yarn.keytab"])

  sc_conf.set('spark.yarn.principal', custom_conf["spark.yarn.principal"])

  sc_conf.set('spark.sql.catalogImplementation', custom_conf["spark.sql.catalogImplementation"])

  sc_conf.set('spark.executor.instances', custom_conf["spark.executor.instances"])

 

  try:

      sc.stop()

      sc = SparkContext(conf=sc_conf)

  except:

      sc = SparkContext(conf=sc_conf)

 

  spark = SparkSession(sc)

 

server = '******'

username = '******'

password = '******'

driver= '{ODBC Driver 17 for SQL Server}'

tableName=''*****'

conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE=*****;UID='+username+';PWD='+ password)

 

SQL= 'SELECT *  FROM {}'.format(tableName)

SQL_Query = pd.read_sql_query(SQL, conn)

pdf = pd.DataFrame(SQL_Query)

sdf = spark.createDataFrame(pdf)

conn.close()

pdf.to_csv('/home/filename.csv',index=False,sep='|')

###########################

import os

from impala.dbapi import connect

from datetime import datetime

from datetime import   timedelta

os.system('get_ticket')

def load_data(cursorImpala):

               SQL="""

                                             """

               print("Truncate load ....")

    print(SQL)

    cursorImpala.execute(SQL)   

    print("Data loaded.")

    return

#############################

def refresh_tables(cursorImpala,table_name):

    print(" REFRESH IMPALA TABLE "+table_name)

    cursorImpala.execute("refresh "+table_name)   

    return

#############################

connImpala = connect(host='******************',

              port=21051,

               timeout=1000,

               auth_mechanism='GSSAPI',

               use_ssl=True)

cursorImpala = connImpala.cursor()

refresh_tables(cursorImpala)

load_data(cursorImpala)

#############################

import pyodbc

server = str(os.environ["SERVER"]).strip()

username = str(os.environ["USERNAME"]).strip()

password = str(os.environ["PASSWORD"]).strip()

 

print server

print username

print password

 

driver= '{ODBC Driver 17 for SQL Server}'

 


cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE=*****;UID='+username+';PWD='+ password)

 

dwcursor = cnxn.cursor()

dwcursor.execute("truncate table  dbo.tablename")

dwcursor.commit()

dwcursor.close()

cnxn.close()

#################################

Load data into pandas dataframe

from impala.dbapi import connect

from impala.util import as_pandas

 

connImpala = connect(host='***********',

               port=*****,

               timeout=1000,

               auth_mechanism='GSSAPI',

               use_ssl=True)

cursorImpala = connImpala.cursor()

 

cursorImpala = connImpala.cursor()   

cursorImpala.execute(sqlcmd)

pricevalueRows = as_pandas(cursorImpala)

#####################################################3

from impala.dbapi import connect

import os

from datetime import datetime

from datetime import date

from datetime import timedelta

import base64

def insert_into_table(table_name,from_date,to_date):

               sql_query=""" INSERT OVERWRITE """+table_name+""" PARTITION (datekey) """+""" SELECT

               """

    return sql_query

###################################################

impala_host=os.environ['IMPALA_HOSTNAME']

from_date=str(date.today()-timedelta(days=60)).replace(" ", "")

to_date=str(date.today()).replace(" ", "")

table_name=''

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"AUTHENTICATION...CONNCTING TO KERBEROS...")

os.system('get_ticket')

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"CONNECTING TO IMPALA PROD...")

connImpala = connect(host=impala_host,

               port=21051,

               timeout=1000,

               auth_mechanism='GSSAPI',

               use_ssl=True) 

cursorImpala = connImpala.cursor()

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"CONNECTED TO IMPALA ...")

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"CREATING TABLE:")

insert_stmt=insert_into_table(table_name,from_date,to_date)

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"EXECUTING QUERY:")

print(insert_stmt)

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"INSERTING DATA: "+ " FROM "+ str(from_date)+ " TO "+ str(to_date))

cursorImpala.execute(insert_stmt)

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"LOADING COMPLETED .")

connImpala.close()

########################################################

#BOM DATA

project_name='bom'

base_dir='file:///notebook/shared/projects/repo-data/'+project_name

stage_dir=base_dir+ "/Stage"

archive_dir=base_dir+"/Archived"

processed_dir=base_dir+'/Processed'

input_dir=processed_dir+'/'+'observation/daily_history/'

landing_db='aiexploration'

HDFS_USER='aboloor'

HDFS_LOC='/'+landing_db+'/'+HDFS_USER+'/'+project_name

import os

from pyspark.sql import SQLContext

from pyspark.sql import SparkSession

from pyspark import SparkContext, HiveContext

from pyspark import SparkConf

from pyspark.sql.types import *

from pyspark.sql.functions import lit

import base64

import pandas as pd

from pandas import DataFrame

from impala.dbapi import connect

from pyspark.sql import functions

import datetime

from datetime import datetime

###############################################

spark_session_sc=init_spark_context(app_name='Spark_BOM_Daily_Ingest_App')

spark_s=spark_session_sc[1]

spark_sc=spark_session_sc[0]

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+' SPARK CONTEXT CREATED.')

print(datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+' '+spark_sc.version)

##################################################

_schema = StructType([StructField('station_id', StringType(), True),

                      StructField('station_name', StringType(),True),

                      StructField('state', StringType(), True),

                      StructField('observation_date', StringType(), True),

                      StructField('evapo_transpiration_0000_2400_mm', StringType(), True),

                      StructField('rain_0900_0900_mm', StringType(), True),

                      StructField('pan_evaporation_0900_0900_mm', StringType(), True),

                      StructField('max_temperature_c', StringType(), True),

                      StructField('min_temperature_c', StringType(), True),

                      StructField('max_rel_hum_percent', StringType(), True),

                      StructField('min_rel_hum_percent', StringType(), True),

                      StructField('average_10m_wind_speed_m_per_sec', StringType(), True),

                      StructField('solar_radiation_mj_per_sqm', StringType(), True)])

df_daily_observations=spark_s.read.options(inferSchema='False',delimiter='|').csv(input_dir+'*.psv', header=False,schema=_schema)

view='bom_daily_observations_vw'

df_daily_observations.createOrReplaceTempView(view)

df_daily_observations.printSchema()

df_daily_observations.toPandas()

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+' CREATING HIVE TABLE...')

write_spark_dataframe_into_impala_table(spark_s,df_daily_observations,db_name,tb_name)

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+' TABLE '+db_name+'.'+tb_name+ ' CREATED SUCCESSFULLY.')

close_spark_session(spark_s,spark_sc,'Spark_BOM_Daily_Ingest_App')

##############################33

Spark Util:

import os

from pyspark.sql import SQLContext

from pyspark.sql import SparkSession

from pyspark import SparkContext, HiveContext

from pyspark import SparkConf

import base64

import os

import pandas as pd

from pandas import DataFrame

from impala.dbapi import connect

from pyspark.sql import functions

#Initialises and return a Spark context based on given mode, Note:Remember app_name for the Spark Context to close it once done

def init_spark_context(_mode="local[*]",_core="2",_driver_memory="2g",_sql_catalogue="hive",app_name="Spark-local"):  #"yarn" for cluster_mode

    os.system('get_ticket')

    os.system('klist')

   with open('/tmp/user.keytab') as fd:

        keytab = base64.b64encode(fd.read())

    with open('/etc/krb5.conf') as fd:

        krbconf = base64.b64encode(fd.read())

    conf = SparkConf()

    conf \

    .setAppName(app_name) \

    .setMaster(_mode) \

    .set("spark.cores.max", _core) \

    .set("spark.driver.memory", _driver_memory) \

    .set("spark.sql.catalogImplementation", _sql_catalogue)

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)

    print("current Spark version:"+sc.version)

    return [sc,spark]

from pyspark.sql import SparkSession

from pyspark import SparkContext, HiveContext

#Writes a pandas dataframe to a Hive/Impala table using given spark session

def write_spark_dataframe_into_impala_table(spark,sdf,db_name,table_name):

    #Create schema for Spark Dataframe

    sdf.createOrReplaceTempView(table_name+'_temp_view')

    print("db_name:"+db_name+'\ntable_name:'+table_name)

    spark.sql('DROP TABLE IF EXISTS '+db_name+'.'+table_name)

    query='CREATE TABLE '+db_name+'.'+table_name+' AS SELECT * FROM '+table_name+'_temp_view'

    print(query)

    spark.sql(query)

    return True

#Creates a spark dataframe  out of given input csv file

from pyspark.sql import functions

def load_csv_into_sparkdf(in_path,spark_session,infer_schema, _delimiter):

    sdfData = spark_session.read.csv(in_path, header=True, inferSchema=infer_schema, delimiter=_delimiter)#inferSchema = True, skip file headers

    return sdfData

#Write spark Dataframe into CSV

def write_sprkDF_to_csv(df,file_name,separator):

    df.write.format('com.databricks.spark.csv').save(file_name,sep=separator)

    return True

def close_spark_session(spark,sc,_app):

    #sc = SparkContext.getOrCreate()

    spark.stop()

    sc.stop()

###################################

#1. Create Spark Context and Session

               from pyspark.sql import SQLContext

from pyspark.sql import SparkSession

from pyspark import SparkContext, HiveContext

from pyspark import SparkConf

import base64

import os

import pandas as pd

from pandas import DataFrame

from impala.dbapi import connect

from pyspark.sql import functions

spark_session_sc=init_spark_context(_mode="local[*]",_core="2",_driver_memory="2g",_sql_catalogue="hive",app_name=spark_app_name)

spark_s=spark_session_sc[1]

spark_sc=spark_session_sc[0]

print(spark_sc.version)

#2. Create Impala Connection¶

impala_host=os.environ['*****']

connImpala = connect(host=impala_host,

                       port=21051,

                       timeout=1000,

                       auth_mechanism='GSSAPI',

                       use_ssl=True)

cursorImpala = connImpala.cursor()

#3. Read Input Files, Convert to Pipe-Delimited and Save as Hive/Impala Tables on HDFS Landing zone

# import os

from pandas import DataFrame

from impala.dbapi import connect

from pyspark.sql import functions

i=0

for index,row in df_raw_files.iterrows():

    input_path=row["raw_file_afs_path"]

    file_name=row["file_name"]

    table_name=row["table_name"]

    print input_path

    print  file_name

    print table_name

    df = spark_s.read.load("file://"+input_path,

                     format="csv", sep=",", inferSchema=infer_schema, header=has_header)

    df.printSchema()

    write_spark_dataframe_into_impala_table(spark_s,df,landing_db,table_name)

    cursorImpala.execute('INVALIDATE METADATA '+landing_db+'.'+table_name)

    cursorImpala.execute('REFRESH '+landing_db+'.'+table_name)

    print "Impala landing table: "+table_name +" created successfully."

    output_piped_file=processed_dir+file_name.partition('.')[0]+"_"+datetime.now().strftime("%Y%m%d")+"_"+datetime.now().strftime("%H%M%S")+"."+file_name.partition('.')[2]

    print "Saving pipe-delimited file at:"+ outputpipedfile

    df.coalesce(1).write.mode('append').format('com.databricks.spark.csv').save("file://"+processed_dir,sep='|',header = 'true')

    cmd='rm -f '+processed_dir+"/"+"_SUCCESS"

    print("DELETING SPARK OUTPUT TEMPORARY FILE")

    print(cmd)

    os.system(cmd)

    cmd='mv -- '+processed_dir+"/*.csv "+processed_dir+"/"+output_piped_file

    print(cmd)

    os.system(cmd)

    print output_piped_file + " SAVED SUCCESSFULLY!

######################

#4. Close Spark Session

close_spark_session(spark_s,spark_sc,spark_app_name)

cursorImpala.close()

connImpala.close()

######################33

 

import pandas as pd

import os

from impala.dbapi import connect

from datetime import datetime

############################

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"CREATING AFS FOLDERS:")

os.system('mkdir'+' ' +"'"+processed_dir+"'")

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+ processed_dir+ " CREATED.")

os.system('mkdir'+ ' '+"'"+archive_dir+"'")     

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+ archive_dir+ " CREATED.")

################

#df_raw_files_columns=['file_name','raw_file_afs_path','table_name','source_name']

df_raw_files=pd.DataFrame(columns=df_raw_files_columns)

#project_name/stage/source/table_name/files...

raw_files_directory = stage_dir

print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"SCANNING AFS INPUT FOLDER")

i=0

j=0

k=0

file_index=0

for path in os.listdir(raw_files_directory):

    full_path = os.path.join(raw_files_directory,path)

    print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"SOURCE NAME["+str((i+1))+"]: "+path)

    for dirs in os.listdir(full_path):

        print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"TABLE NAME ["+str((j+1))+"]: "+dirs)

        full_dir=os.path.join(full_path,dirs)

        j+=1

        for files in os.listdir(full_dir):

            file_path=os.path.join(full_dir,files)

            print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"FILE NAME  ["+str((k+1))+"]: "+files)

            print (datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S:%f] ")+"FILE FULL PATH: "+ file_path)

            df_raw_files.loc[file_index]=[files,file_path, dirs,path]

            k+=1

            file_index+=1

        k=0

    j=0

    i+=1

#########################################33

#haversin formula

def DistanceOf(lat1,lon1,lat2,lon2):

    theta= lon1 - lon2

    dist=dist = math.sin(deg2rad(lat1)) * math.sin(deg2rad(lat2)) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.cos(deg2rad(theta))

    dist = math.acos(dist);

    dist = rad2deg(dist);

    dist = dist * 60 * 1.1515;

    dist = dist * 1.609344;

    return dist

#######################################33

#Return  , address, Longitude Latitude

#input:  or address

import json

import googlemaps

from datetime import datetime

def get_site_geo_location(address):

    geocode_result=get_GoogleMap_Geocode(address)

    site_geo_components=get_location_geometry_components(geocode_result)

    If(site_geo_components is None)

    return None

    return[site_name,site_geo_components[1],site_geo_components[0]['location']['lat'],site_geo_components[0]['location']['lng']]

               #############################################

               df=spark.read.json(sc.wholeTextFiles(path).values())

               #Select break objects into separate fields

               df1=df.select(fields)

               import matplotlib.pyplot as plt

import numpy as np

 

countscreated = data['DateCreatedMonth'].value_counts().sort_index(ascending = True)

countscompleted = data['DateCompletedMonth'].value_counts().sort_index(ascending = True)

 

x = data.DateCreatedMonth.unique()

x = np.sort(x)

print(x)

plt.figure(figsize=(15,10))

plt.bar(x, countscreated, align='center', alpha=0.5)

plt.xticks(x)

plt.xlabel('Month Created')

plt.ylabel('Count of Month Created')

plt.show()

#######################################3

import plotly.offline as py

from plotly.offline import init_notebook_mode

import plotly.graph_objs as go

import pandas as pd

from pandas import DataFrame

import datetime

####################################################

def draw_grouped_barchart(dataset1,dataset2,title_, x_axis_, y_axis_,trace1_name,trace1_x_index,trace1_y_index,trace2_name,trace2_x_index,_trace2_y_index,_width=800,_height=600):

    init_notebook_mode(connected=True)

    trace1=go.Bar( x=dataset1[dataset1.columns.values[trace1_x_index]],

                               y=dataset1[dataset1.columns.values[trace1_y_index]],

                  text=dataset1[dataset1.columns.values[trace1_y_index]],

                  name=trace1_name

                  

    )

    trace2=go.Bar( x=dataset2[dataset2.columns.values[trace2_x_index]],

                               y=dataset2[dataset2.columns.values[trace1_y_index]],

                  text=dataset2[dataset2.columns.values[trace1_y_index]],

               

                  name=trace2_name

    )

    data=[trace1,trace2]

    layout = go.Layout(

    title=title_,

        font=dict(

            family='Calibri, monospace',

            size=12,

            color='#000000'

        ),

    barmode='group',

          xaxis=dict(

            title=x_axis_,

            titlefont=dict(

                family='Courier New, monospace',

                size=12,

                color='#7f7f7f'

            )

        ),

        yaxis=dict(

            title=y_axis_,

            titlefont=dict(

                family='Courier New, monospace',

                size=12,

                color='#7f7f7f'

            )

        )

    )

    fig = go.Figure(data=data, layout=layout)

    py.iplot(fig, filename='grouped-bar')