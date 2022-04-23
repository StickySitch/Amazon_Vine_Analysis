#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
# Find the latest version of spark 3.0 from http://www.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.3'
spark_version = 'spark-3.0.3'
os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
get_ipython().system('apt-get update')
get_ipython().system('apt-get install openjdk-11-jdk-headless -qq > /dev/null')
get_ipython().system('wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop2.7.tgz')
get_ipython().system('tar xf $SPARK_VERSION-bin-hadoop2.7.tgz')
get_ipython().system('pip install -q findspark')

# Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop2.7"

# Start a SparkSession
import findspark
findspark.init()


# In[ ]:


# Download the Postgres driver that will allow Spark to interact with Postgres.
get_ipython().system('wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar')


# In[ ]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("M16-Amazon-Challenge").config("spark.driver.extraClassPath","/content/postgresql-42.2.16.jar").getOrCreate()


# ### Load Amazon Data into Spark DataFrame

# In[ ]:


from pyspark import SparkFiles
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Furniture_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8").csv(SparkFiles.get("amazon_reviews_us_Furniture_v1_00.tsv.gz"), sep="\t", header=True, inferSchema=True)
df.show()


# ### Create DataFrames to match tables

# In[ ]:


from pyspark.sql.functions import to_date
# Reading in the Review dataset as a DataFrame
# Dataframe is already established: "df"


# In[ ]:


# Creating the customers_table DataFrame
customerDf = df.groupBy("customer_id").agg({"customer_id":"count"}).withColumnRenamed("count(customer_id)", "customer_count")

# Displaying the customerDf dataframe
customerDf.show()


# In[ ]:


df.select(['product_id','product_title']).count()


# In[ ]:


# Create the products_table DataFrame and drop duplicates. 
productsDf = df.select(['product_id','product_title']).drop_duplicates(['product_id','product_title'])

# Displaying the products tables
productsDf.show()


# In[ ]:


productsDf.dropna()


# In[ ]:


productsDf.count()


# In[ ]:


# Create the review_id_table DataFrame. 
# Convert the 'review_date' column to a date datatype with to_date("review_date", 'yyyy-MM-dd').alias("review_date")
reviewIdDf = df.select(['review_id','customer_id', 'product_id','product_parent', to_date("review_date", 'yyyy-MM-dd').alias("review_date")])

# Displaying reviewId table
reviewIdDf.show()


# In[ ]:


# Create the vine_table. DataFrame
vineDf = df.select(['review_id', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase'])

# Displaying vineDf
vineDf.show()


# ### Connect to the AWS RDS instance and write each DataFrame to its table. 

# In[ ]:


# Configure settings for RDS
mode = "append"
jdbc_url="jdbc:postgresql://<YOUR AWS DB ENDPOINT HERE>.amazonaws.com:5432/<YOUR DB NAME HERE>"
config = {"user":"postgres", 
          "password": "<YOUR PASS HERE>", 
          "driver":"org.postgresql.Driver"}


# In[ ]:


# Write review_id_df to table in RDS
reviewIdDf.write.jdbc(url=jdbc_url, table='review_id_table', mode=mode, properties=config)


# In[ ]:


reviewIdDf.count()


# In[ ]:


# Write products_df to table in RDS
# about 3 min
productsDf.write.jdbc(url=jdbc_url, table='products_table', mode=mode, properties=config)


# In[ ]:


# Write customers_df to table in RDS
# 5 min 14 s
customerDf.write.jdbc(url=jdbc_url, table='customers_table', mode=mode, properties=config)


# In[ ]:


# Write vine_df to table in RDS
# 11 minutes
vineDf.write.jdbc(url=jdbc_url, table='vine_table', mode=mode, properties=config)


# In[ ]:




