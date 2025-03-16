#!/usr/bin/env python
# coding: utf-8

# In[6]:


import numpy as np
import pandas as pd
from functools import reduce
import warnings
warnings.filterwarnings("ignore")

def data_handling(df):
    # Remove duplicates in 'AccountId' column
    df.drop_duplicates(subset=['AccountId'], inplace=True)
    # Handling missing values
    df.dropna(subset=['AccountId'], inplace=True)
    return df

topics_list = ['ai', 'gaming', 'history', 'movies', 'music', 'softwareengineering']
data_path = "/Users/thanmaireddyk/Desktop/StackExchange_csv/"

# Create an empty DataFrame to store results
results_df = pd.DataFrame(columns=topics_list, index=topics_list)

# Iterate through each pair of topics
for i in range(len(topics_list)):
    for j in range(i+1, len(topics_list)):
        topic1 = topics_list[i]
        topic2 = topics_list[j]

        # Read the CSV files for each pair of topics
        df1 = pd.read_csv(f"{data_path}{topic1}.stackexchange.com/Users.csv")
        df2 = pd.read_csv(f"{data_path}{topic2}.stackexchange.com/Users.csv")

        df1 = data_handling(df1)
        df2 = data_handling(df2)
        
        # Calculate common values
        common_values = pd.merge(df1, df2, on='AccountId', how='inner')

        # Calculate the percentage of common users
        total_users_topic1 = len(df1)
        total_users_topic2 = len(df2)
        total_common_users = len(common_values)
        
        # Calculate percentage
        percentage_topic1_with_topic2 = (total_common_users / total_users_topic1) * 100
        percentage_topic2_with_topic1 = (total_common_users / total_users_topic2) * 100

        # Update results in the DataFrame
        results_df.loc[topic1, topic2] = f"{percentage_topic1_with_topic2:.2f}%"
        results_df.loc[topic2, topic1] = f"{percentage_topic2_with_topic1:.2f}%"

results_df.fillna('100%', inplace=True) 

# Display the results
print(results_df)



# ### Active users across all 6 topics:

# In[13]:


# Create a dictionary to store processed dataframes for each topic
processed_data_dict = {}
total_users = 0

for topic in topics_list:
    # Load Users.csv for the current topic
    df = pd.read_csv(f"/Users/thanmaireddyk/Desktop/StackExchange_csv/{topic}.stackexchange.com/Users.csv")
    # Process data for the current topic
    df = data_handling(df)
 
    print(f'Total number of users in {topic}: {len(df)}')
    total_users = total_users + len(df)    
   
    # Store processed data in the dictionary
    processed_data_dict[topic] = df

# Perform inner join across all dataframes on the 'AccountId' column
merged_data = reduce(lambda left, right: pd.merge(left, right, on='AccountId', how='inner'), processed_data_dict.values())

# Display the results
print(f'There are {len(merged_data)} number of users who are active in all the 6 topics.')
print(f'There are a total of {(total_users-len(merged_data)*5)} users in all the 6 topics.')


# In[ ]:




