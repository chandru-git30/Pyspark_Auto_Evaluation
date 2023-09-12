from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

def session_f():
    spark  = SparkSession.builder.appName("pyspark_new_1").getOrCreate()
    return spark

#1.appName 
def appName_f():
    return session_f().sparkContext.appName

#2.Master
def master_f():
    return session_f().sparkContext.master

#3.limit 5 with show
def limit_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.limit(5).show(truncate=False)

#4.row count
def row_count_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.count()

#5.column name
def column_name_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.columns

#6.select column
def select_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.select("gender","lunch").show()

#7.describe
def describe_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.describe(["math score"]).show()
#8.summary
def summary_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.select("reading score").summary("min","max").show()
#9.sort
def sort_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.sort(col("writing score").asc()).select("writing score").show()
#10.lit
def lit_f():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.withColumn("age",lit(25)).show()