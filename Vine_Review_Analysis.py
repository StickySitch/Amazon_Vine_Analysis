#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[31]:


# Download the Postgres driver that will allow Spark to interact with Postgres.
get_ipython().system('wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar')


# In[32]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("M16-Amazon-Challenge2").config("spark.driver.extraClassPath","/content/postgresql-42.2.16.jar").getOrCreate()


# In[33]:


from pyspark import SparkFiles
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Furniture_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding","UTF-8").csv(SparkFiles.get("amazon_reviews_us_Furniture_v1_00.tsv.gz"), sep="\t", header=True, inferSchema=True)
df.show()


# In[34]:


# Checking that the data types are correct
df.dtypes


# In[35]:


# Getting reviews with total_vote count that is >=20
greaterThan20 = df.filter("total_votes>=20")
greaterThan20.show()


# In[36]:


greaterThan20.count()


# In[37]:


helpfulVotes = greaterThan20.filter("helpful_votes/total_votes >=0.5")
helpfulVotes.show()


# In[38]:


helpfulVotes.count()


# In[39]:


# Creating a Datafram contain those that are PAID Vine program users
# Success can be confirmed by looking at the "vine" column values (They will all be "Y")
vinePaidDf = helpfulVotes.filter("vine=='Y'")
vinePaidDf.show()


# In[40]:


# Creating a Datafram contain those that are UNPAID Vine program users
# Success can be confirmed by looking at the "vine" column values (They will all be "N")
vineUnpaidDf = helpfulVotes.filter("vine=='N'")
vineUnpaidDf.show()


# In[41]:


# Getting the total count of unpaid and paid vine users
totalReviewsPaid = vinePaidDf.count()
totalReviewsUnpaid = vineUnpaidDf.count()

# Getting count of 5 star ratings of unpaid and paid vine users
fiveStarPaid = vinePaidDf.filter("star_rating==5").count()
fiveStarUnpaid = vineUnpaidDf.filter("star_rating==5").count()

# Getting the percentage of 5 star users for paid and unpaid vine users
fiveStarPaidPerc = fiveStarPaid / totalReviewsPaid * 100
fiveStarUnpaidPerc = fiveStarUnpaid / totalReviewsUnpaid * 100


# In[42]:


print("Total number of 'Helpful' paid vine reviews: %f.1" % totalReviewsPaid)
print("Total number of 'Helpful' UNPAID vine reviews: %f.1" % totalReviewsUnpaid)
print('\n')

print('Number of "Helpful" 5-star reviews by paid vine users: %.1f' % fiveStarPaid)
print('Number of "Helpful" 5-star reviews by UNPAID vine users: %.1f' % fiveStarUnpaid)
print('\n')

print('Percentage of 5-star reviews by paid vine users: %.2f' % fiveStarPaidPerc + "%")
print('Percentage of 5-star reviews by UNPAID vine users: %.2f' % fiveStarUnpaidPerc + "%")


# In[42]:





# In[43]:


import pandas as pd
import matplotlib.pyplot as plt

# Covnverting all PySpark DataFrames to Pandas DataFrames to make creating
# our visualizaions easier.
pdVinePaidDf = vinePaidDf.toPandas()
pdVineUnpaidDf = vineUnpaidDf.toPandas()
pdHelpfulDf = helpfulVotes.toPandas()



pdVinePaidDf.head()


# # Charting Results For README.md

# In[44]:


barHelpfulDf = pdHelpfulDf[['vine', 'customer_id']]
barHelpfulDf.head()


# In[45]:


import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
plt.style.use('ggplot')


# In[46]:


# function to add value labels to bar chart
def addlabels(x,y):
    for i in range(len(x)):
        plt.text(i, y[i], y[i], ha = 'center')


# In[47]:


import numpy as np

# creating color list to assign color to each bar
colors = ["r","g"]
# Getting the count of paid and unpaid reviewers useing "barHelpfulDf" Dataframe
pltDf = barHelpfulDf.vine.value_counts()

# Using numpy to use the length of our "pltDf.index" list as x axis positions
# for our bars
x_pos = np.arange(len(pltDf.index))

fig,axs = plt.subplots(figsize=(6,6))

# Creating bar chart
axs.bar(x_pos, pltDf.values, align="center", color=colors)
# Setting x axis bar label position
axs.set_xticks(x_pos)
# Setting x axis bar labels
axs.set_xticklabels(pltDf.index.unique())
# Setting bar chart title
axs.set_title("Paid (Y) Vs. Unpaid (N) Reviewers")
# Setting x axis label
axs.set_xlabel("Vine Status")
# Setting y axis label
axs.set_ylabel("User Count")
# Adding value count to the top of each bar
addlabels(pltDf.index, pltDf.values)

plt.show()


# In[48]:


# Function to display pie slice count and percentage
def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
    return my_autopct

# Getting the amount of each star rating for PAID reviewers
pltPaidDf = pdVinePaidDf.star_rating.value_counts()

myLabels = ["5 Stars", "4 Stars", "3 Stars", "2 Stars"]
# Highlighting the 5 star slice by seperating(exploding) it from the pie
explode = (0.1, 0.0, 0.0, 0.0,)


fig, axs = plt.subplots(figsize=(8,8))

_, texts, autotexts = axs.pie(pltPaidDf.values, 
                              explode=explode, 
                              labels=myLabels,
                              startangle = 90, 
                              shadow = True,
                              autopct=make_autopct(pltPaidDf.values),
                              pctdistance=.55,
                              wedgeprops = {'linewidth': 3})

# loop to set color for text of pie slices
for autotext in autotexts:
    autotext.set_color('black')
# Sets font size for slice labels
[ _.set_fontsize(15) for _ in texts ]

# Setting pie chart title
axs.set_title('Star Ratings For Vine Users')

# Displaying pie chart
plt.show()


# In[50]:


pltUnpaidDf = pdVineUnpaidDf.star_rating.value_counts()
pltUnpaidDf


# In[59]:


# Function to display pie slice count and percentage
def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.2f}%  ({v:d})'.format(p=pct,v=val)
    return my_autopct

# Getting the amount of each star rating for UNPAID reviewers
pltUnpaidDf = pdVineUnpaidDf.star_rating.value_counts()

myLabels = ["5 Stars", "4 Stars", "3 Stars", "2 Stars", "1 Star"]
# Highlighting the 5 star slice by seperating(exploding) it from the pie
explode = (0.1, 0.0, 0.0, 0.0, 0.0)

fig, axs = plt.subplots(figsize=(8,8))
_, texts, autotexts =axs.pie(pltUnpaidDf.values,
                             explode=explode, 
                             labels=myLabels,startangle = 90, 
                             shadow = True,
                             autopct=make_autopct(pltUnpaidDf.values), 
                             pctdistance=.55,
                             wedgeprops = {'linewidth': 3})

# loop to set color for text of pie slices
for autotext in autotexts:
    autotext.set_color('black')

# Sets font size for slice labels   
[ _.set_fontsize(15) for _ in texts ]

# Sets pie chart table
axs.set_title('Star Ratings For Regular Users')

# Displaying pie chart
plt.show()

