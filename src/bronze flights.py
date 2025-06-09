# Databricks notebook source

df = spark.read.option("header", "true").csv("/Volumes/raw/flights/us_flights/")

# Write to the table
df.coalesce(1).write.mode("overwrite").saveAsTable("bronze.flights.us_flights")
