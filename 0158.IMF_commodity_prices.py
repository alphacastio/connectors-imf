#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


#Como el link va cambiando, se busca en la pagina de commodities

page = requests.get('https://www.imf.org/en/Research/commodity-prices')
soup = BeautifulSoup(page.content, 'html.parser')


# In[3]:


#Buscamos el link del excel
link_xls = []

for link in soup.find_all('a'):
    if 'Excel Database' in link.get_text():
        link_xls.append('https://www.imf.org' + link.get('href'))


# In[4]:


#Descargamos el archivo de excel
xls_file = requests.get(link_xls[0])
df = pd.read_excel(xls_file.content)


# In[5]:


new_header = df.iloc[0] #grab the first row for the header
df = df[3:] #take the data less the header row
df.columns = new_header #set the header row as the df header


# In[6]:


df["Date"] = pd.to_datetime(df["Commodity.Description"], errors="coerce", format="%YM%m")
df = df.set_index("Date")
del df["Commodity.Description"]
df["country"] = "Global"

del df['Crude Oil (petroleum), Price index, 2016 = 100, simple average of three spot prices; Dated Brent, West Texas Intermediate, and the Dubai Fateh']

alphacast.datasets.dataset(158).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



