# Python Notebook - Understanding Search at Yammer

datasets

#import packages and set up dataframe from datasets table
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
#form a dataframe from the Weekly Search Share query
daily_searches = datasets['Weekly Search Share']
daily_searches.head()

#add the search fraction to the dataframe and plot the search fraction
daily_searches['search_fraction'] = (100*daily_searches['searches']/(daily_searches['searches']+
                                     daily_searches['other_traffic']))
daily_searches.plot(x='search_time',y='search_fraction', title='Weekly Average Search Percent', figsize=(9,6))
plt.show()
print(daily_searches['search_fraction'].describe())

#To define sessions first split apart the users from the event table into respective user events tables.
#Also, filter non search related events (filtered via query)
search_events = datasets['All search events'].sort_values('user_id').reset_index(drop=True)

#Find unique users
users = search_events.user_id.unique()

#separate users into a list of dataframes by events by user
users_dfs = ([search_events.loc[search_events['user_id'] == user].sort_values('occurred_at').reset_index(drop=True)
             .drop(['device','event_type','location','user_id','user_type'], axis=1) for user in users])
assert len(users_dfs) == len(users)

users_dfs[0].head()

#Label sessions by labeling user activity by 10 min inactivity gaps
import datetime as dt
session = 0
delta = dt.timedelta(minutes = 10)
for df in users_dfs:
  for index, row in df.iterrows():
    if index:  
      if row['occurred_at'] - df.loc[index - 1,'occurred_at'] < delta:
        df.loc[index, 'session'] = session
      else:
        df.loc[index, 'session'] = session + 1
        session += 1
    else:
      df.loc[index, 'session'] = session
      df.loc[index, 'initial'] = 1
  session += 1      

#Bring together separate users into one dataframe and then group by session
session_df = pd.concat(users_dfs, axis=0, ignore_index=True)
session_counts = (session_df.pivot_table(index='session',columns=['event_name'],values='occurred_at',
              aggfunc='count',margins=True))

#create timestamps for sessions by grabbing the first timestamp of each session.
#Then add those timestamps to the session_counts dataframe as a column.
#Then make timestamps the index for the dataframe.
session_counts_time = session_df.groupby('session').min().drop(['event_name','initial'],axis=1)
session_counts_time = pd.concat([session_counts,session_counts_time],axis=1)
session_counts_time = session_counts_time.set_index('occurred_at', drop=True).sort_index()

#First, group the data weekly and look at the time series of weekly usage of search features.
weekly_session_counts = session_counts_time.resample('W').count()
weekly_session_counts.plot(y=['search_autocomplete','search_run'], title='Weekly Sessions with Searches',figsize=(9,6))
plt.ylabel('Sessions')
plt.show()

#Plot percent of search traffic weekly.
percent_auto = weekly_session_counts.search_autocomplete/weekly_session_counts.All
percent_run  = weekly_session_counts.search_run/weekly_session_counts.All
percent = pd.concat([percent_auto,percent_run],axis=1)
percent.columns=['percent_auto','percent_run']
percent.plot(figsize=(9,6))
plt.ylabel('% Traffic Share')
plt.title('Traffic Share per Week')
plt.show()

#Make a function to plot a bar chart of value counts(histogram) of a given Series.
def bar_plot(Series):
  return (Series.value_counts().sort_index().plot(kind='bar', figsize=(9,6),width=.8))

#To obtain full searches per session, look at histograms of full searches per session
bar_plot(session_counts.search_run[:-1])
plt.xlabel('Full Searches per Session')
plt.ylabel('Sessions')
plt.title('Number of Sessions with Full Searches')
plt.show()

#To obtain autocomplete searches per session, look at histograms of autocomplete searches per session
bar_plot(session_counts.search_autocomplete[:-1])
plt.xlabel('Autocomplete Searches per Session')
plt.ylabel('Sessions')
plt.title('Number of Sessions with Autocomplete Searches')
plt.show()

#Selecting only sessions with full searches.
runs = session_counts.loc[session_counts['search_run']>0].drop(['search_autocomplete','All'], axis=1).fillna(0)
runs['click_total'] = runs.filter(like='click',axis=1).sum(axis=1)
bar_plot(runs.click_total[:-1])
plt.xlabel('Clicks in Session')
plt.ylabel('Number of Sesstions')
plt.title('Clicks per Session with at Least One Full Search')
plt.show()

#Look into average clicks grouped by full searches per session.
runs_by_session = runs.groupby(by='search_run').mean().filter(like='click_total', axis=1)
runs_by_session.iloc[slice(0,-1),0].plot(kind='bar',figsize=(9,6),width=0.8, legend=False)
plt.xlabel('Full Searches per Session')
plt.ylabel('Average Clicks per Session')
plt.title('Average Clicks per Session by Full Searches per Session')

#Now to look at the clicks by result ordering when full search is used at least once.
click_counts = pd.melt(runs.drop(['click_total','search_run'], axis=1))
click_counts = click_counts.loc[click_counts.value>0].groupby(by='event_name').count()
click_counts.index = [int(str(index)[20:]) for index in click_counts.index.tolist()]
click_counts.sort_index(inplace=True)
click_counts.value.plot(kind='bar', figsize=(9,6),width=0.8)
plt.xlabel('Full Search Result Order')
plt.ylabel('Clicks')
plt.title('Clicks by Full Search Result')
plt.show()

#set up the user dfs to reflect monthly repeat search usage
users_search=([df.loc[(pd.isnull(df.initial) & df.event_name.str.contains('search_run'))].drop(['initial','event_name'],axis=1).set_index('occurred_at').resample('M').count() 
       for df in users_dfs])
users_auto=([df.loc[(pd.isnull(df.initial) & df.event_name.str.contains('search_autocomplete'))].drop(['initial','event_name'],axis=1).set_index('occurred_at').resample('M').count() 
       for df in users_dfs])

#Looking into monthly usage by users after the first usage
search_avg = pd.DataFrame(pd.concat(users_search,axis=1).count(axis=1))
search_avg.columns = ['full_search']
auto_avg = pd.DataFrame(pd.concat(users_auto,axis=1).count(axis=1))
auto_avg.columns = ['autocomplete']
pd.concat([search_avg,auto_avg],axis=1).plot(figsize=(9,6))
plt.xlabel('Date')
plt.ylabel('Searches')
plt.title('Repeat Monthly Search Usage')
plt.show()

