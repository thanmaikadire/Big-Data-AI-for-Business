#!/usr/bin/env python
# coding: utf-8

# In[5]:


import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt
import seaborn as sns


num_posts_list = []
topics_list = ['ai', 'gaming', 'history', 'movies', 'music', 'softwareengineering']

for topic in topics_list:
    # Load Posts.csv for the current topic
    posts_df = pd.read_csv(f"/Users/thanmaireddyk/Desktop/StackExchange_csv/{topic}.stackexchange.com/Posts.csv")
    # Get the number of posts
    num_posts = len(posts_df)
    num_posts_list.append(num_posts)
    
# Create a DataFrame for visualization
data = {'Topic': topics_list, 'Number of Posts': num_posts_list}
df = pd.DataFrame(data)
# Sort the DataFrame by 'Number of Posts' in descending order
df = df.sort_values(by='Number of Posts', ascending=False)
# Create a bar plot using Seaborn
plt.figure(figsize=(12, 6))
sns.barplot(data=df, x='Topic', y='Number of Posts', palette='viridis')
plt.xlabel('Topic')
plt.ylabel('Number of Posts')
plt.title('Number of Posts per Topic')
plt.tight_layout()
# Save the plot
plt.savefig("Posts_topic.png")
# Show the bar plot
plt.show()


# In[ ]:




