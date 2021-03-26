# Report - Group 06  

## Introduction

This document represents an overview of our project. We start with its architecture and then delve into each major component and give a short description for it, along with the technologies used.  

The main idea behind the project is to determine which of the following characteristics: *day, time, state* and *length* influence the sentiment of a tweet the most. We chose to use [this dataset](https://archive.org/details/twitter_cikm_2010) for our project, since every tweet contains a timestamp and a geotag for the time and location it was sent from, respectively. To accomplish our goal we perform various map/reduce steps on our dataset in order to group the tweets based on the aforementioned characteristics and compute the mean sentiment for each group. Finally, we use the **two-way ANOVA** test to determine which of our metrics contribute the most to the variance of the sentiment score. 

## Architecture

The architecture of the application follows the structure presented in the diagram below.

![Alt text](./Architecture/SC_Architecture.png?raw=true "Title")

The arrows show the dataflow of our architecture. The tweets are retrieved from the database by the Spark drivers. We have two spark drivers, one responsible for the historical data and one responsible for the streaming data. The former retrieves the data using a simple SQL query, while the latter gets it via a message queue. Both drivers then send the tweets to the Spark cluster for processing.

The processing consists of ... list + detail processing steps here

After the processing is done, the drivers store the results in the initial database.

### Database
**Technologies:** Elasticsearch, Python  
**Description:** The database is responsible for storing the inital dataset of tweets as well as the results of the processing pipeline. The results should ideally be stored in a separate database, but this approach helps us to have a less resource-hungy development environment. However, the initial data as well as the various results are separated in their individual Elasticsearch indices for convenience, ease-of-access and portability. For implementing the database itself we chose to use Elasticsearch because it is distributed by nature, document-oriented (so there's no schema that needs to be defined and updated throughout the development process), fast and easily scalable, plus the fact that it's something none of us worked with before, and this project looked like a good opportunity. For populating the database we used Elasticsearch's Python library, which is a wrapper for the RESTful API of Elasticsearch. This allowed us to abstract and streamline most of the querying process and the pushing data in parallel process.

### Message queue

### Spark driver - historical

### Spark driver - streaming

### Spark cluster
**Technologies:** Spark, PySpark  
**Description:** Spark is a distributed data processing engine. It lets us process big data sets faster by splitting the work up into chunks and assigning those chunks across a number of workers. The cluster used for our development process is formed by one Spark leader and 2 Spark worders. Its integrated map/reduce implementation stands at the core of our processing pipeline. We chose to use Spark over the alternatives because it is faster, well-documented and well-integrated in Python via the PySpark library.

