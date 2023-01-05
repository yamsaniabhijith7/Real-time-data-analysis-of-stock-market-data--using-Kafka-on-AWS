#!/usr/bin/env python
# coding: utf-8

# In[21]:


from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json


# In[22]:


pip install s3fs


# In[23]:


from s3fs import S3FileSystem


# In[24]:


consmr = KafkaConsumer(
    'demo_test',
     bootstrap_servers=['3.133.129.33:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))


# In[25]:


#for c in consumer:
#    print(c.value)


# In[26]:


s3 = S3FileSystem()


# In[ ]:


for count, i in enumerate(consmr):
    with s3.open("s3://yee-kafka/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)  


# In[ ]:





# In[ ]:




