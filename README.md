# Amazon_Vine_Analysis
## Overview
When looking at the Amazon marketplace, we can see that there are THOUSANDS of reviews for each product in each category. Amazon has a service called **Vine** where users are are essentially "paid" (sometimes in the form of products) reviewers. Our job today was to use `PySpark` to analyze the `vine review data` to decide if the vine program is a success. In other words, are the paid users of vine creating reliable reviews? Does paying reviewers create higher quality reviews?

### PySpark DataFrames Created & Used:

#### Vine review dataframe

![Vine Review DF](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/vineReviewersDf.png)

#### Non-vine review dataframe

![Non-vine Review DF](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/nonVineReviewersDf.png)

## Resources
- `Software/IDE:` [Google Colaboratory](https://colab.research.google.com/) 
- `Data Sources:` [Amazon Reviews Dataset](https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt)
- `Libraries:` [PySpark](https://spark.apache.org/docs/latest/api/python/#) **|** [Pandas](https://pandas.pydata.org/) **|** [Matplotlib](https://matplotlib.org/)
- `Source Code:` [Vine_Review_Analysis.ipynb](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Vine_Review_Analysis.ipynb)

## Results

#### Review Dataset Category: `Furniture (US)`

As mentioned right above, the category of product reviews we are analyzing is `Furniture (US)`. The goal for today was to answer the following questions:
- **How many Vine reviews and non-Vine reviews were there?**
	- **Vine reviews:** `136`
	- **Non-vine reviews:** `18,019`
	
- **How many Vine reviews were 5 stars?**
	- **Vine reviews:** `74`
	
- **How many non-Vine reviews were 5 stars?**
	- **Non-vine reviews:** `8,482`
	
- **What percentage of Vine reviews were 5 stars?**
	- **Vine Reviews 5-Star Percentage :** `54.41%`
	
- **What percentage of non-Vine reviews were 5 stars?**
	- **Non-vine Reviews 5-Star Percentage :** `47.07%`


**`SPOILER RESULTS FROM PYSPARK DATAFRAME`**

![All Output](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/allOutPut.png)

I added some `spoilers` to the questions above but below we are going to break it down a little more. Let's get started!

### 1. How many Vine reviews and non-Vine reviews were there?
In order to  get a good idea of our dataset, we needed to separate our non-vine users from the vine users and get the amount of reviews from these user categories. This gives us a very basic break down of the data distribution.


![barHelpfulDf dataframe](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/barHelpfulDf.png)

To ensure quality users, we filtered our original dataframe to users with 20+ reviews. Using that dataframe which we called `pdHelpfulDf`, a new dataframe called `barHelpfulDf` was created; Which consists of our reviewers **vine** status and **customer_id** as seen above. **NOTE:** `The dataframe above does not diplay any "Y" values in the "vine" column due to there being so few in the dataframe.`

![Review Type Counts](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/paidvsunpaid.png)

As you can see, the vine user review count heavily outweighs the regular users. Ideally there would be more vine user data points because with our current count being so low, the smallest increase could drastically change the results.

  - **Vine Review Count:** `136`
	  - There are `136` reviews by vine (PAID) users. As mentioned above, this data set would be larger ideally.
  - **Non-vine Review Count:** `18,019`
	  - There are `18,019` reviews by vine (UNPAID) users. 

Next we will breakdown the **star ratings** and their **percentages** for both the vine and non-vine user reviews; And I've got the perfect visualization for it! A pie chart!

### 2a. How many Vine reviews were 5 stars? What percentage of Vine reviews were 5 stars?

Below is a pie chart displaying the star rating breakdown for the reviews left by vine users. As you can see, not only are we able to see the amount of 5 star ratings and its percentage, we can also see the 1-4 star rating counts and their percentage makeup. **NOTE:** `1 star ratings are not accounted for due to there being non present.`

![Vine Pie Chart](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/vineUserReviewsStarRating.png)

- **`5 Star`**
	- Reviews: `74`
	- Percentage: `54.41%`
- **`4 Star`**
	- Reviews: `45`
	- Percentage: `33.09%`
- **`3 Star`**
	- Reviews: `15`
	- Percentage: `11.03%`
- **`2 Star`**
	- Reviews: `2`
	- Percentage: `1.47%`
- **`1 Star`**
	-  `No 1 Star reviews present in vine review data.`
		- Reviews: `0`
		- Percentage: `0%`

### 2b. How many non-Vine reviews were 5 stars? What percentage of non-Vine reviews were 5 stars?

Now let's look at the same breakdown for our non vine reviews and see if there is a difference. Remember, the goal here is to see if paying reviewers for their time is worth it. 

![Non-vine Pie Chart](https://github.com/StickySitch/Amazon_Vine_Analysis/blob/main/Resources/Images/regularReviewsStarRating.png)

- **`5 Star`**
	- Reviews: `8,482`
	- Percentage: `47.07%`
- **`4 Star`**
	- Reviews: `3,483`
	- Percentage: `19.33%`
- **`3 Star`**
	- Reviews: `3,098`
	- Percentage: `17.19%`
- **`2 Star`**
	- Reviews: `1,680`
	- Percentage: `9.32%`
- **`1 Star`**
	- Reviews: `1,276`
	- Percentage: `7.08%`

As you can see from both data sets, as mentioned earlier; The review counts are drastically different. Keep that in mind. With that in mind though, we can see that the **vine reviews** have **7.34%** more **5 Star reviews.** Along side the additional 7.34% 5-star reviews, there is also no presence of 1-Star values. Possibly some positivity bias happening here. 

## Summary

This analysis definitely needs further investigation and more data points in order to decide if Amazons vine paid reviewer program is worth it.

1. Increase Vine Review Data Set Size
	- In order to accurately compare the two datasets (vine vs non-vine) we need many more data points for the vine review data set. As of now, the slightest change can drastically effect the data. 

2. Use A Linear Regression Module To Evaluate The Validity of Each Review
	- Another big issue (possibly) with the comparison is positivity bias. Are these users leaving better reviews based on the fact that they are being paid? Or are they basing their reviews off of the product itself? Linear regression would help us determine the validity of each vine users review by comparing it to the non-vine review data set. Adding more data points to the vine users would also help determine the review accuracy. Would there still be a 7.34% increase in 5-star ratings if the vine user data set had `18,019` data points? 
