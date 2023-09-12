from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import session

def session_f():
    spark  = SparkSession.builder.appName("pyspark_new_1").getOrCreate()
    return spark
session_f()

#1.appName 
def appName_t():
    return session_f().sparkContext.appName

#2.Master
def master_t():
    return session_f().sparkContext.master

#3.limit 5 with show
def limit_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.limit(5).show(truncate=False)
#4.row count
def row_count_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.count()
#5.column name
def column_name_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.columns
#6.select column
def select_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.select("gender","lunch").show()
#7.describe
def describe_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.describe(["math score"]).show()
#8.summary
def summary_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.select("reading score").summary("min","max").show()
#9.sort
def sort_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.sort(col("writing score").asc()).select("writing score").show()
#10.lit
def lit_t():
    df = session_f().read.csv("students.csv",inferSchema=True,header=True)
    return df.withColumn("age",lit(25)).show()

########################################TESTING###################################################### 

#appName_test
def test_appName():
    assert session.appName_f() == appName_t()

#master_test
def test_master():
    assert session.master_f() == master_t()
 
#limit_test 
def test_limit():
    assert session.limit_f() == limit_t()
    
#row_count_test
def test_rowcount():
    assert session.row_count_f() == row_count_t()  

#column_name_test
def test_column_name():
    assert session.column_name_f() == column_name_t()

#select_test
def test_select():
    assert session.select_f() == select_t()

#describe_test
def test_describe():
    assert session.describe_f() == describe_t()

#summary_test
def test_summary():
    assert session.summary_f() == summary_t()

#sort_test
def test_sort():
    assert session.sort_f() == sort_t()

#lit_test
def test_lit():
    assert session.lit_f() == lit_t()