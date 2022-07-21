#!/usr/bin/env python
# coding: utf-8

# In[9]:


pip install pyspark


# In[10]:


import pyspark


# In[11]:


from pyspark.sql import *
spark=SparkSession.builder       .appName("load DF to Hive")       .enableHiveSupport()       .getOrCreate()


# In[18]:


#creating a dataframe for demog dataset
dev_demog_df= spark.read.csv("/user/unextbdh22id09/Capstone2/dev_demog.csv", header=True)
dev_demog_df.show(5)


# In[19]:


#creating a database 
spark.sql("create database cp2_db")


# In[20]:



dev_liability_df= spark.read.csv("/user/unextbdh22id09/Capstone2/dev_liability.csv", header=True)
dev_liability_df.show(5)


# In[21]:


dev_bureau_df= spark.read.csv("/user/unextbdh22id09/Capstone2/dev_liability.csv", header=True)
dev_bureau_df.show(5)


# In[24]:


dev_demog_df.write.mode("overwrite").saveAsTable("cp2_db.dev_demog")


# In[28]:


dev_liability_df.write.mode("overwrite").saveAsTable("cp2_db.dev_liability")


# In[29]:


dev_bureau_df.write.mode("overwrite").saveAsTable("cp2_db.dev_bureau")


# In[30]:


spark.sql("select * from cp2_db.dev_demog limit 10").show()


# In[31]:


spark.sql("select * from cp2_db.dev_liability limit 5").show()


# In[32]:


spark.sql("select * from cp2_db.dev_bureau limit 5").show()


# In[ ]:




