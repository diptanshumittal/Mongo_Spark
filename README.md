# Mongo_Spark

MongoDb stores the data as a collection of documents where each document is stored in BSON format and referred to one record. Using BSON approach enables to store arrays without the need of any external table.There were a total of 273.1k documents with average size of documents being 121B. Total size of the collection was 31.6MB. 
All the keys and values in the document are stored in the form of a string.  
For MongoDB, a mongo-spark-connector to connect spark with the database and send the query in the form of a pipeline to the database engine. Results were loaded into a data frame.

File1.py - Loads the database from MongoDB database and into the spark dataframe and queries the dataframe using pysparl.sql.functions. 

File2.py - Sends NoSQL queries to MongoDB engine from the spark session and load result into a dataframe.

