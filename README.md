
# Application

  This application analyzes a financial fraud detection dataset and determines the types of transactions with the 
  most frauds, along with the largest and smallest fraudulent transaction amounts for each such transaction.

  Spark Structured Streaming provides functionality to work with real-time streaming data. This application leverages
  such stream processing and applies it to the financial domain.

  We start out with using a portion of the dataset in a csv file and ingest it to a kafka topic. Then, we stream
  the data from that kafka topic and process the streaming data to determine the transaction types with the most 
  frauds, along with the largest and smallest fraud amounts for every such transaction. Finally, the result of this
  analysis is output to another kafka topic.


# Dataset 

  This application uses a financial fraud detection dataset (see link below). The dataset contains information on 
  which transactions where fraudulent (the isFraud column) along with relevant information, including source,
  amount, destination and transaction type.

  You can obtain the dataset and find out more through this link

  https://www.kaggle.com/datasets/ealaxi/paysim1


# CSV File

  For simplicity, this application only used a portion of the dataset. Specifically, the application used the rows
  of the dataset from 718 onwards, numbering around 1214 total rows. This part of the dataset was copied manually
  and pasted into a csv file. This csv file was then ingested into a kafka topic to be used for program input.


# Writing the csv file to kafka topic

  You can write the csv file to a kafka topic with the following command

  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic inputtopicname < /path/to/file/file.csv


# Command line arguments 

  This application required 3 command line arguments

  1 - Kafka input topic ( --input)

  2 - Kafka output topic ( --output)

  3 - Directory to store checkpoints when storing results in a kafka topic ( --check)

  Pass these arguments when running the application


# How to run the program

  You can run the program with the following command, modifying it according to your own versions, topic names and 
  directory

  bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 fraud_analytics.py --input inputtopicname --output  outputtopicname --check /path/to/directory/ 

  This command assumes that kafka servers are running, the dataset has been ingested to inputtopicname, and that this
  command is executed in a spark directory


# Libraries

  This application uses the following libraries:

  1 - pyspark

  This library provides apache spark functionality for python. This application uses it for several tasks, including
  reading from/writing to kafka, constructing a dataframe and processing streaming data.

  2 - argparse

  This library provides the functionality to use command-line arguments in a user-friendly way.

  These libraries can be obtained with the following commands

  pip install pyspark
 
  pip install argparse



# Version

  This application was built on

  kafka 2.8
  python 3.8
  spark 3.3.0
  ubuntu 20.04
