#!/usr/bin/env python
# coding: utf-8

# In[3]:


pip install kafka-python


# In[33]:


import pandas as pd
from kafka import KafkaConsumer,KafkaProducer
from time import sleep
from json import dumps
import json


# In[34]:


prod = KafkaProducer(bootstrap_servers=['3.133.129.33:9092'],
                         api_version=(0,11,5),#change ip here
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# In[35]:


prod.send('yee',value={'h':'par'})


# In[36]:


df = pd.read_csv("C:/Users/yamsa/data engineering projects/indexProcessed.csv")


# In[37]:


df.head()


# In[ ]:


while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    prod.send('demo_test', value=dict_stock)
    sleep(1)


# In[ ]:


prod.flush() #clear data from kafka server


# In[ ]:




