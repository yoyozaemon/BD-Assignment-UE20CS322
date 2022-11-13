#!/usr/bin/env python3
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import sys

args = sys.argv

conf = SparkConf().setAppName("bigdata_assignment_2").setMaster("yarn")
sp = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(args[1], inferSchema=True, header=True)

df = df.na.drop()
avgFineOfEachReg = df.groupBy("RP State Plate").agg({'Fine amount': 'mean'})
avgFineOfEachReg = avgFineOfEachReg.withColumnRenamed("RP State Plate", "state")
df = df.join(avgFineOfEachReg, df["RP State Plate"] == avgFineOfEachReg['state'])
df = df.select(["RP State Plate", "Color", "Fine amount", "Ticket number", "avg(Fine amount)"])

df = df.filter(((df["Color"] == "WH") & (df['Fine amount'] >= func.round(df["avg(Fine amount)"], 2)))) \
    .select("Ticket number")\
    .distinct()\
    .orderBy("Ticket number")

df.write.csv(args[2])
