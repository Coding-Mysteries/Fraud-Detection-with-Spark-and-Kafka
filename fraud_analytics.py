
# This application analyzes a fraud detection dataset and determines the type of transactions with the most frauds,
# along with the largest and smallest fraud amounts in each such transaction.

# This file streams the dataset from a kafka topic, performs the analysis on this data, and stores the result
# to another kafka topic

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse


# Some basic spark set up

spark = SparkSession.builder.master("local[4]").appName("Fraud Analytics").getOrCreate()

# This is to avoid the log messages that completely fill the screen

spark.sparkContext.setLogLevel("ERROR")

# We take input from the command line for 2 kafka topic names. One is the topic from which we stream the dataset.
# The other is the topic in which we store the results. We also take as input the directory in which to store
# checkpoints when writing the results to the kafka topic

parser = argparse.ArgumentParser(description = "kafka topic names for input and output")
parser.add_argument("-i", "--input", metavar = "", required = True, help = "name of the topic to ingest data from")
parser.add_argument("-o", "--output", metavar = "", required = True, help = "name of the topic to get data from")
parser.add_argument("-c", "--check", metavar = "", required = True, help = "directory to store checkpoints in")
args = parser.parse_args()

input_topic = args.input
output_topic = args.output
checkpoint_directory = args.check


# We read in the dataset from the kafka topic into a streaming dataframe

dataframe = spark\
        .readStream\
        .format("kafka")\
        .option("startingOffsets", "earliest")\
        .option("subscribe", input_topic)\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .load()


# The dataframe is changed into the desired format, containing all the columns of the dataset

dataframe = dataframe.select(col("value").cast("string")).alias("csv").select("csv.*")


dataframe = dataframe.selectExpr(\
        "split(value,',')[0] as step" \
        ,"split(value,',')[1] as type" \
        ,"split(value,',')[2] as amount" \
        ,"split(value,',')[3] as nameOrig" \
        ,"split(value,',')[4] as oldbalanceOrg" \
        ,"split(value,',')[5] as newbalanceOrig" \
        ,"split(value,',')[6] as nameDest" \
        ,"split(value,',')[7] as oldbalanceDest"\
        ,"split(value,',')[8] as newbalanceDest"\
        ,"split(value,',')[9] as isFraud" \
        ,"split(value,',')[10] as isFlaggedFraud" \
        )

# Getting the fraudulent transactions

dataframe = dataframe.filter( col("isFraud") == 1)

# Getting the transaction types with the most frauds, along with the largest and smallest fraud amount 
# for each such transaction

dataframe = dataframe.select("type", "amount", "isFraud")

dataframe = dataframe.groupBy("type")\
        .agg( count("isFraud").alias("Number of Frauds"), max("amount").alias("Biggest Fraud"), \
        min("amount").alias("Smallest Fraud"))

dataframe = dataframe.filter( col("Number of Frauds") > 1)


# Formatting the results in preparation for storage in a kafka topic

dataframe = dataframe.select( concat( lit("Type: "), "type", lit("  Number of Frauds: "), "Number of Frauds",\
        lit("  Biggest Fraud: "), "Biggest Fraud", lit("  Smallest Fraud: "), "Smallest Fraud").alias("value"))


# Storing the results in a kafka topic

dataframe\
        .writeStream\
        .outputMode("complete")\
        .format("kafka")\
        .option("topic", output_topic)\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("checkpointLocation", checkpoint_directory)\
        .start()\
        .awaitTermination()




