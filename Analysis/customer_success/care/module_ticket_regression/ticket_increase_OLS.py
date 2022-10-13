import os
import sys
import numpy as np
import pandas as pd
import warnings
from datetime import date, datetime
import statsmodels.api as sm
from IPython.core.display import display, HTML
import snowflake.connector
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')
display(HTML("<style>.container { width:100% !important; }</style>"))
pd.set_option('display.max_rows', 550)
pd.set_option('display.max_columns', 550)
import snowflake.connector
cnx = snowflake.connector.connect(
    user='joanna.karavolias@toasttab.com',
    account='toast.us-east-1',
    authenticator='externalbrowser'
    )


def QueryDWH(query, cnx, col=[]):
    cur = cnx.cursor()
    cur.execute(query)
    # If there is no col input, then get the columns from the return of the
    # snowflake query
    if not col:
        df = pd.DataFrame(cur.fetchall())
        if not df.empty:
            df.columns = [desc[0] for desc in cur.description]
    else:
        df = pd.DataFrame(cur.fetchall(), columns=col)

    return df
QueryDWH("""USE DATABASE TOAST""", cnx)
QueryDWH("""USE WAREHOUSE TOAST_WH""", cnx)


query = '''
with end_modules as (
select
    salesforce_accountid,
    pos_first_order_date,
    module,
    iff(is_activated,1,0) as is_activated
from analytics_core_arr.daily_module_activation_adoption
where  dt  = '2022-06-30'
    and module ilike any ('gift card program monthly subscription','loyalty program monthly subscription','marketing monthly subscription','online ordering monthly subscription','scan to pay','toast order & pay','toast delivery service subscription','third party delivery integrations package','grubhub integration','doordash integration','ubereats integration')
    and pos_status ilike 'live' and saas_status ilike 'live'
    and pos_first_order_date < '2022-01-01'
    group by 1,2,3,4
    )

, start_modules as (
select
    salesforce_accountid,
    CASE WHEN datediff(day,pos_first_order_date,dt) <= 90 THEN '0-3 months' else '3+ Months' end  as age_bucket,
    COUNT(CASE WHEN IS_ACTIVATED = true then 1 else null end) as start_activated_modules
from analytics_core_arr.daily_module_activation_adoption
where dt  = '2022-01-01'
    and module ilike any ('gift card program monthly subscription','loyalty program monthly subscription','marketing monthly subscription','online ordering monthly subscription','scan to pay','toast order & pay','toast delivery service subscription','third party delivery integrations package','grubhub integration','doordash integration','ubereats integration')
    and pos_status ilike 'live' and saas_status ilike 'live'
group by 1,2
    )

, tickets as (
select
    salesforce_accountid,
    count(salesforce_caseid) as tickets_created
from cs_customer_care.support_ticket t
where
created_datetime >= '2022-01-01' and created_datetime < '2022-07-01'
group by 1)

, final as (
select
    em.*,
    age_bucket,
    start_activated_modules,
    tickets_created
from end_modules em
left join start_modules sm
    on em.salesforce_accountid = sm.salesforce_accountid
left join tickets t
    on em.salesforce_accountid = t.salesforce_accountid
order by 1,2
    )
select * from final
;
'''

test.columns

test = QueryDWH(query,cnx)
# Manipulate data for for OLS
df_wide = pd.pivot(test,index = ['SALESFORCE_ACCOUNTID','POS_FIRST_ORDER_DATE','START_ACTIVATED_MODULES','TICKETS_CREATED','AGE_BUCKET'],columns = 'MODULE', values = 'IS_ACTIVATED')

df_wide = df_wide.reset_index()
df_wide = df_wide.fillna(0)

df_wide.head()
df_wide['END_ACTIVATED_MODULES'] = df_wide.iloc[:,3:14].sum(axis=1)
df_wide['modules_added'] = df_wide['END_ACTIVATED_MODULES'] - df_wide['START_ACTIVATED_MODULES']


df_wide.columns = ['salesforce_accountid', 'first_order_date', 'start_act_mod','tickets_created','age_bucket','dd','gc','grub','loy','tem','oo','stp','3pd','tds','opt','uber','end_act_mod','mod_added']


df_wide['integrations'] = np.where(df_wide['dd'] + df_wide['grub'] + df_wide['uber']>1,1,0)
df_wide.head()
cols = ['gc','loy','tem','oo','stp','tds','opt','3pd','dd','grub','uber','integrations']
df_wide[cols] = df_wide[cols].astype('category')


less3 =  df_wide[df_wide['age_bucket']== '0-3 months']
plus3 = df_wide[df_wide['age_bucket']== '3+ Months']
# Linear Regression
import scipy.stats as stats
from sklearn.model_selection import train_test_split
from sklearn import metrics
import seaborn as sns
from imblearn.over_sampling import SMOTE
import statsmodels.api as sm

from statsmodels.miscmodels.ordinal_model import OrderedModel

#Split data into training and test group
## ALl data
y = df_wide['tickets_created']
feature_cols = ['end_act_mod','mod_added','gc','loy','tem','oo','stp','tds','opt','integrations']
x = df_wide[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

print(results.summary())

# Less than 3 monthsy = df_wide['tickets_created']
y = less3['tickets_created']
feature_cols = ['end_act_mod','mod_added','gc','loy','tem','oo','stp','tds','opt','integrations']
x = less3[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

print(results.summary())


# More than 3 months
y = plus3['tickets_created']
feature_cols = ['end_act_mod','mod_added','gc','loy','tem','oo','stp','tds','opt','integrations']
x = plus3[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

print(results.summary())

#QA Checks
# Sum activations across all Modules
df_wide['grub'].value_counts()
len(df_wide)
df_wide['salesforce_accountid'].value_counts()
