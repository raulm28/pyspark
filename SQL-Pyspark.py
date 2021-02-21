import pyspark
import sys
import os
import pyodbc
import pandas as pd
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as func
import matplotlib.pyplot as plt
from pyspark.sql.types import Row, StringType, LongType, DoubleType, StructField, StructType, TimestampType, IntegerType
from openpyxl import Workbook
from datetime import datetime

pd.set_option('display.max_rows',150000)
pd.set_option('display.max_columns',10)
pd.set_option('display.width', 100000)

os.environ['HADOOP_HOME'] = 'C:\Hadoop'
sys.path.append("C:\Hadoop\\bin")
#writer = ExcelWriter()
cnx = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                'Server=SCISPSQLRDS;'
                                 'Database=Prod;'
                                'Trusted_Connection=yes;')

cursor = cnx.cursor()

f1 = open('G:\My Documents\SQL Server Management Studio\SCM-LVPOrders.sql','r',encoding='utf-16')
query = pd.read_sql_query(f1.read(),cnx)
df = pd.DataFrame(query)#.sort_values(by=['PharmacyName','Unit','VerifyDate'])
#print(df)
sc = SparkContext()
sql = SQLContext(sc)
spark = SparkSession.builder.appName("Query").getOrCreate()

iva = sql.createDataFrame(df)
iva_pivot = iva.groupBy('VerifyDate','VerifyHour','MRN','VisitID','CurrentLocation','Service','OrderNum','Name','SummaryLine','FlowRate','PrimaryGI','Reorders').pivot('add#').agg(func.first('Additives').alias('Additive'))
iva_data = iva_pivot.coalesce(1)
iva_data.toPandas().to_excel("G:\My Documents\\iva_data.xlsx",sheet_name='Data',index=False)

'''ver_schema = StructType([StructField('PharmacyName',StringType(),True),
                          StructField('VerifyDate',StringType(),True),
                          StructField('VerifyHour',StringType(),True),
                          StructField('Unit',StringType(),True),
                          StructField('Unit1',StringType(),True),
                          StructField('OrdersVerified',IntegerType(),True)])
rever_schema = StructType([StructField('OrderNum',StringType(),True),
                          StructField('CreatedWhen',TimestampType(),True),
                          StructField('MRN',StringType(),True),
                          StructField('Location',StringType(),True),
                          StructField('OrderName',StringType(),True),
                          StructField('AttributeName',StringType(),True),
                          StructField('OldValue',StringType(),True),
                          StructField('NewValue',StringType(),True),
                          StructField('CreatedBy',StringType(),True)])

verdf = sql.createDataFrame(df,rever_schema)
#verdf.show(1000)
#ver_pivot = verdf.groupBy('PharmacyName','Unit','VerifyDate').pivot('VerifyHour').agg(func.first('OrdersVerified')).fillna('').orderBy('PharmacyName','VerifyDate','Unit')
ver_pivot = verdf.groupBy('OrderNum', 'CreatedWhen','CreatedBy', 'MRN', 'Location','OrderName').pivot('AttributeName').agg(func.first('OldValue').alias('OldValue'),func.first('NewValue').alias('NewValue'))#.orderBy('')
#ver_data = ver_pivot.coalesce(1).fillna('').withColumn('CreatedWhen',func.from_utc_timestamp('CreatedWhen','America/New_York')).orderBy('CreatedWhen','OrderNum')#.show(1000)
ver_data = ver_pivot.coalesce(1).fillna('')
#ver_data.show(1000)
#ver1_pivot = ver_data.groupBy('PharmacyName','Unit','13:00:00', '01:00:00', '11:00:00', '20:00:00', '05:00:00', '16:00:00', '03:00:00', '02:00:00', '08:00:00', '19:00:00', '10:00:00', '22:00:00', '06:00:00', '07:00:00', '04:00:00', '17:00:00', '09:00:00', '21:00:00', '15:00:00', '12:00:00', '14:00:00', '23:00:00', '18:00:00', '00:00:00').pivot('VerifyDate').agg(func.first('Unit'))
#ver1_data = ver1_pivot.coalesce(1).show()
#ver_data.write.option("header","true").csv("G:\My Documents\\ver_data.csv")
ver = ver_data.toPandas()
plt.figure()
ver.plot(x='OrderName',y=['GenericItemName_OldValue'],kind="scatter")
plt.xticks(rotation=90)
plt.show()
#ver_data.toPandas().to_excel("G:\My Documents\\ver_data.xlsx",sheet_name='Data',index=False)'''

