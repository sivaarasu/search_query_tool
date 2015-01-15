
# coding: utf-8

# In[1]:

from rocketads.oauth2.credentials import get_default_production_client
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.adwords.hierarchy import get_mcc_hierarchy_raw
from rocketads.utils.misc import get_date_in_the_past, change_logger_level, remove_logger_handlers
from rocketads.utils.misc import is_valid_broad_modified

#Display more information on the download process
change_logger_level('rocketads.adwords','INFO')
_logger=remove_logger_handlers()
del _logger.handlers[0]

#Top level MCC for Uganda
TOP_MCC_FOODPANDA_UGANDA = 1270737969


# In[2]:

#The first step is to create a client for the top level mcc
client = get_default_production_client(TOP_MCC_FOODPANDA_UGANDA)

#This datatructure holds all sub mccs for the country
graph = get_mcc_hierarchy_raw(client, return_top_level=False)


# In[3]:

all_accounts = {customer.name: customer.customerId for customer in graph.entries}

all_accounts={name:val for name,val in all_accounts.items() if 'SEM' in name and ('LVN' in name or 'LPH' in name or 'LID' in name or 'LTH' in name or 'LSG' in name or 'LMY' in name)}


# In[4]:

all_accounts


# In[5]:

#We look for all history up to now
end_time = get_date_in_the_past(0)
start_time = get_date_in_the_past(7)


# In[6]:

query = """
        SELECT AdGroupId, AdGroupName,CampaignId,CampaignName, CampaignStatus, Clicks, Impressions,
        KeywordTextMatchingQuery, Query, Cost, ConversionValue, MatchType, Conversions 
        FROM SEARCH_QUERY_PERFORMANCE_REPORT
        WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED
        DURING {}, {}
        """.format(start_time, end_time)


# In[7]:

#The query will be executed in parallel for all sub mccs in uganda
df = get_report_for_accounts(query, all_accounts.values(), nthreads=20)
#The return value is a dataframe which concatenates the results of the queries in each sub account


# In[8]:

from rocketads.utils.string_processing import TextNormalizer
Norm = TextNormalizer()


# In[9]:

df['search_term_norm'] = df.search_term.apply(Norm.normalize)


# In[10]:

df


# In[11]:

import pandas as pd


# In[12]:

agg_data_frame = []
for key, group in df.groupby(['search_term_norm']):
    tot_cost=group.cost.sum()
    tot_impressions=group.impressions.sum()
    tot_conv_clicks = group.converted_clicks.sum()
    tot_value=group.total_conv_value.sum()
    d={'search_term_norm':key, 'tot_impressions':tot_impressions,
       'tot_cost': tot_cost,
       'tot_conv_clicks':tot_conv_clicks,
       'tot_value':tot_value }
    agg_data_frame.append(d)
    
    
agg_data_frame=pd.DataFrame(agg_data_frame)


# In[13]:

agg_data_frame[agg_data_frame.tot_conv_clicks > 0]


# In[ ]:




# In[ ]:




# In[14]:




# In[14]:

df2 = pd.merge(df,agg_data_frame,how='left',on=['search_term_norm'])


# In[15]:

df2


# In[16]:

from forex import forex
from datetime import date
fx = forex()
to_currency = 'PHP'
from_currency = 'SGD'
dt = str(date.today())


# In[17]:

m_price = fx.get_currency(dt,from_currency,to_currency)


# In[18]:

m_price


# In[19]:

df2['tot_cost_conv'] = df2['tot_cost'] * m_price


# In[20]:

df2


# In[21]:

df2['tot_cir'] = df2['tot_cost_conv']/df2['tot_value']


# In[24]:

df2[(df2.tot_cir > 0) & (df2.tot_cir != pd.np.inf)]


# In[28]:

camp_multiplier=6
ag_multiplier= 3
cir_treshold=0.35
cost_treshold=1e6

def is_bad(row):
    if (row['tot_value']!= 0) and row['tot_cir'] >= camp_multiplier* cir_treshold:
        return 'CAMP_NEGATIVE--'
    elif (row['tot_value']!= 0) and row['tot_cir'] >= ag_multiplier* cir_treshold:
        return 'AG_NEGATIVE'
    elif (row['tot_value']== 0) and row['tot_cost'] >= camp_multiplier* cost_treshold:
        return 'CAMP_NEGATIVE'
    elif (row['tot_value']== 0) and row['tot_cost'] >= ag_multiplier* cost_treshold:
        return 'AG_NEGATIVE'
    else:
        return 'FINE'
    


# In[29]:

df2['is_bad']=df2.apply(is_bad,axis=1)


# In[30]:

3*0.35


# In[32]:

df2[df2.is_bad != 'Fine']


# In[ ]:




# In[18]:




# In[19]:




# In[12]:

grouped = result.groupby('match_type')


# In[13]:

grouped.sum()


# In[17]:

get_ipython().system(u'pwd')


# In[ ]:



