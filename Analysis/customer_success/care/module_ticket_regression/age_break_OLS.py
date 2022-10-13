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
with live_devices as (
-- live devices
select
date_trunc(month,event_timestamp)::date as event_month,
c.salesforce_accountid,
count(distinct serial_number) as start_devices,
lead(start_devices) over (partition by salesforce_accountid order by event_month asc) as end_devices,
end_devices - start_devices as net_device_change
from source_splunk.hardware_app_version_current dev
left join analytics_core.toastorders_customer_bridge tcb
    on dev.restaurant_guid = tcb.toastorders_restaurant_guid
left join analytics_core.customer c
    on tcb.customer_id = c.customer_id
where event_month = '2022-01-01' or event_month = '2022-06-01'
group by 1,2
)

, end_mods as (
-- number of active modules on last day of period that can be pivoted to boolean
select
salesforce_accountid,
module,
iff(is_activated,1,0) as is_activated,
activation_date,
case when activation_date >= '2022-01-01' and activation_date < '2022-07-01' then 1 else 0 end as is_activated_period
from analytics_core_arr.daily_module_activation_adoption
where dt = '2022-06-30'
and pos_status ilike 'live'
and saas_status ilike 'live'
and saas_quantity > 0
and module ilike any ('gift card program monthly subscription','loyalty program monthly subscription','marketing monthly subscription','online ordering monthly subscription','scan to pay','toast order & pay','toast delivery service subscription','third party delivery integrations package','grubhub integration','doordash integration','ubereats integration')
)

, start_mods as (
-- number of modules activated on the first day of period
select
    salesforce_accountid,
    count(case when is_activated then salesforce_accountid end) as start_activated_modules
from analytics_core_arr.daily_module_activation_adoption
where dt ='2022-01-01'
and pos_status ilike 'live'
and saas_status ilike 'live'
and saas_quantity > 0
and module ilike any ('gift card program monthly subscription','loyalty program monthly subscription','marketing monthly subscription','online ordering monthly subscription','scan to pay','toast order & pay','toast delivery service subscription','third party delivery integrations package','grubhub integration','doordash integration','ubereats integration')
group by 1
)

, sample_pop as (
-- identify true population all customers active on Jan 1 2022
select
    salesforce_accountid,
    pos_first_order_date,
    churn_date,
    datediff(day,pos_first_order_date,'2022-01-01') as start_age,
    datediff(month,pos_first_order_date,'2022-01-01') as age_months,
    case when start_age <= 90 then 'Less than 3 Months' else '3+ Months' end as age_bucket
from analytics_core.customer
where pos_first_order_date <= '2022-01-01'
and (churn_date > '2022-01-01' or churn_date is null)
)

, tickets as (
-- tickets per customer
select
    salesforce_accountid,
    count(salesforce_caseid) as ticket_count
from cs_customer_care.support_ticket
where created_datetime >= '2022-01-01' and created_datetime < '2022-07-01'
group by 1
    )

, act_period as (
-- modules activated in period
select
    salesforce_accountid,
    module,
    activation_date,
    case when activation_date >= '2022-01-01' and activation_date < '2022-07-01' then 1 else 0 end as is_activated_2022
from analytics_core_arr.current_module_activation_adoption
where pos_status ilike 'live'
and saas_status ilike 'live'
and saas_quantity > 0
and module ilike any ('gift card program monthly subscription','loyalty program monthly subscription','marketing monthly subscription','online ordering monthly subscription','scan to pay','toast order & pay','toast delivery service subscription','third party delivery integrations package','grubhub integration','doordash integration','ubereats integration')
)

select
    sp.salesforce_accountid
    ,sp.age_months as start_age
    ,sp.age_bucket
    ,t.ticket_count
    ,sm.start_activated_modules
    ,ld.start_devices
    ,ld.end_devices
    ,ld.net_device_change
    ,em.module
    ,em.is_activated
    ,em.is_activated_period
from sample_pop sp
left join tickets t
    on sp.salesforce_accountid = t.salesforce_accountid
left join start_mods sm
    on sp.salesforce_accountid = sm.salesforce_accountid
left join live_devices ld
    on sp.salesforce_accountid = ld.salesforce_accountid
    and ld.event_month = '2022-01-01'
left join end_mods em
    on sp.salesforce_accountid = em.salesforce_accountid
group by 1,2,3,4,5,6,7,8,9,10,11
;
'''

test = QueryDWH(query,cnx)
test.head(10)

df_wide = pd.pivot(test,index = ['SALESFORCE_ACCOUNTID','AGE_BUCKET','START_AGE','TICKET_COUNT','START_ACTIVATED_MODULES','START_DEVICES','END_DEVICES','NET_DEVICE_CHANGE'], columns = 'MODULE', values = 'IS_ACTIVATED')
df_wide.head()
df_wide = df_wide.reset_index()
df_wide = df_wide.fillna(0)
df_wide['end_modules'] = df_wide.iloc[:,8:20].sum(axis=1)
df_wide['modules_added'] = df_wide['end_modules'] - df_wide['START_ACTIVATED_MODULES']
df_wide['3pd'] = np.where(df_wide['DoorDash Integration'] + df_wide['GrubHub Integration']+ df_wide['UberEats Integration'] > 0,1,0)
df_wide['end_mods_sq'] = df_wide['end_modules'] * df_wide['end_modules']
df_wide['end_devices_sq'] = df_wide['END_DEVICES']* df_wide['END_DEVICES']
df_wide['age_sq'] = df_wide['START_AGE']**2


df_wide.columns = ['salesforce_accountid', 'age_bucket','age','ticket_count','start_mods','start_devices','end_devices','net_device','nan','dd','gc','grub','loy','tem','oo','stp','3pd_pack','tds','opt','uber','end_mods','modules_added','3pd','end_mod_sq','end_devices_sq','age_sq']


cols = ['dd','gc','grub','loy','tem','oo','stp','3pd_pack','tds','opt','uber','3pd']
df_wide[cols] = df_wide[cols].astype('category')

#Remove Outliers
q1 = np.percentile(df_wide['ticket_count'], 25, interpolation = 'midpoint')
q3 = np.percentile(df_wide['ticket_count'], 75, interpolation = 'midpoint')
IQR = q3 - q1

upper = df_wide['ticket_count'] >= (q3+1.5*IQR)
lower = df_wide['ticket_count'] <= (q1-1.5*IQR)


less3 =  df_wide[df_wide['age_bucket']== 'Less than 3 Months']
plus3 = df_wide[df_wide['age_bucket']== '3+ Months']

# OLS
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
y = df_wide['ticket_count']
feature_cols = ['age','age_sq','end_mods','modules_added','end_mod_sq','end_devices','end_devices_sq','net_device','gc','loy','tem','oo','stp','tds','opt','3pd']
x = df_wide[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

# Total Population
print(results.summary())

print(results.params)
df_wide.describe()
# 3 Month old customers
y = less3['ticket_count']
feature_cols =['end_mods','modules_added','end_mod_sq','end_devices','end_devices_sq','net_device','gc','loy','tem','oo','stp','tds','opt','3pd']
x = less3[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()
print(results.params)
# Less than 3 Months
print(results.summary())

# 3+ Month old customers
y = plus3['ticket_count']
feature_cols = ['end_mods','modules_added','end_mod_sq','end_devices','end_devices_sq','net_device','gc','loy','tem','oo','stp','tds','opt','3pd']
x = plus3[feature_cols]

x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

#More than 3 Months
print(results.summary())
print(results.params)

plus3.describe()

# Create correlation matrix
import seaborn as sns
import matplotlib.pyplot as plt

corrMatrix = df_wide.drop('nan',axis=1).corr()
sns.heatmap(corrMatrix,annot=True)




x = sm.add_constant(x, prepend = False)
model = sm.OLS(y,x)
results = model.fit()

#More than 3 Months
print(results.summary())
# regress "expression" onto "motifScore" (plus an intercept)
p = model.fit().params

# generate x-values for your regression line (two is sufficient)
x = np.arange(0, 35)

# scatter-plot data
sns.set(rc={'figure.figsize':(11.7,8.27)})
sns.boxplot(
data=df_wide, x='ticket_count',y='end_mods',
    notch = True, showcaps = False,
    flierprops = {"marker":"x"},
    boxprops = {"facecolor":(.4,.6,.8,.5)},
    medianprops={"color":"coral"},
    orient = "h")
plt.xlim(0,100)


df_wide[df_wide['ticket_count']>=200]
