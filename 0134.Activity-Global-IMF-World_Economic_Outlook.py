#!/usr/bin/env python
# coding: utf-8

# In[34]:


import pandas as pd
import requests
import io
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[40]:


url = 'https://www.imf.org/external/pubs/ft/weo/data/WEOhistorical.xlsx'
r = requests.get(url, allow_redirects=False, verify=False)

xl = pd.ExcelFile(url)
sheet_names= xl.sheet_names

dfFinal = pd.DataFrame([])
for i,sheet_name in enumerate(sheet_names[1:]):
    check = i==0
    print(check)
    with io.BytesIO(r.content) as datafr:
        df = pd.read_excel(datafr, sheet_name=sheet_name)
    
    df = df.dropna(how='all', subset= df.columns[1:])
    df.reset_index()
#     def change_date(x):
#         list_x= x.split('/')
#         date = list_x[2]+'-'+list_x[1]+'-'+list_x[0]
#         return date
    
#     df['Date'] = df[df.columns[0]].apply(lambda x: change_date(x))
#     del df[df.columns[0]]
#     df= df.set_index('Date')
    if check==True:
        dfFinal = df
    else:
        dfFinal = pd.merge(dfFinal, df, how='left', on= ['country','WEO_Country_Code','ISOAlpha_3Code','year'])




dfFinal['country'] = dfFinal['country'].apply(lambda x: x.replace('World', 'Global'))



# In[41]:


def replaceDots(x):
    if x=='.':
        x= np.nan
    return x

for col in dfFinal.columns:
    dfFinal[col] = dfFinal[col].apply(lambda x: replaceDots(x))

dfFinal['Date'] = dfFinal['year'].astype(str) + '-12-31'
dfFinal['Date'] = pd.to_datetime(dfFinal['Date'], format='%Y-%m-%d')
# dfFinal = dfFinal.set_index('Date')

df_past = dfFinal[dfFinal['year']<2022]
df_future = dfFinal[dfFinal['year']>2021]
del df_past['year']
del df_future['year']


newCols = []
for column in df_future.columns:
    if (column == 'Date') | (column == 'country') | (column == 'WEO_Country_Code') | (column == 'ISOAlpha_3Code'):
        newCols +=[column]
    else:
        colname = column +'_f'
        newCols += [colname]
        
df_future.columns = newCols

df = pd.concat([df_past, df_future])
df = df.set_index('Date')
df = df.sort_index()


alphacast.datasets.dataset(134).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



