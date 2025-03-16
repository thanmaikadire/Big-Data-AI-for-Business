#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
from nltk.corpus import stopwords
from collections import Counter
from IPython.display import display, Image, HTML

# Initialize Spark Session
spark = SparkSession.builder.appName("Wordcloud Processing").getOrCreate()
topics_list = ['ai', 'gaming', 'history', 'movies', 'music', 'softwareengineering']

def generate_wordcloud(topic):
    # Load Tags.csv for the current topic
    tags_df = pd.read_csv(f"/home/ec2-user/s3_files/StackExchange/{topic}.stackexchange.com/Tags.csv")
    # Create an array with duplicates for each tag based on their count
    repeated_tags = np.concatenate([np.repeat(tag, count) for tag, count in zip(tags_df['TagName'], tags_df['Count'])])
    # Count occurrences of each word
    word_counts = Counter(repeated_tags)
    # Generate a WordCloud object from the word counts
    wordcloud = WordCloud(background_color='white', width=600, height=300, max_font_size=75, max_words=40)
    wordcloud.generate_from_frequencies(word_counts)
    # Save the wordcloud to an image
    wordcloud.to_file(f"{topic}_wordcloud.png")
    # Access the words and their frequencies
    common_words = wordcloud.words_
    print("Top 5 most frequent words for: "+topic)
    # Print the top 5 most common words and their frequencies
    for idx, (word, freq) in enumerate(common_words.items()):
        if idx < 5:
            print(f'{word}: {freq}')
        else:
            break
    print('\n')

# Parallelize the task using Spark and collect results locally
spark.sparkContext.parallelize(topics_list).foreach(generate_wordcloud)
# Display the wordcloud images with titles
for topic in topics_list:
    display(HTML(f"<h2 style='text-align:left;'>Wordcloud for {topic.capitalize()}</h2>"))
    display(Image(filename=f"{topic}_wordcloud.png"))




