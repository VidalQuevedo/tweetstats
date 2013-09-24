tweetstats
==========

A set of MapReduce scripts to get descriptive stats from Twitter objects stored in MongoDB.

# Requirements

To use this script, you need:

1. A (MongoDB)[http://www.mongodb.org/] database with a collection containing BSON documents imported from a (raw Twitter dataset in JSON format)[https://dev.twitter.com/docs/platform-objects/tweets].
2. (PyMongo)[http://api.mongodb.org/python/current/]

# Usage

1. Download tweetStats.py
2. Make sure your MongoDB server is running
3. In the same directory where tweetsStats.py is located, type:

  python tweetStats.py -cm COMMAND -db DATABASE -coll COLLECTION [-regen REGENERATE] [-lim LIMIT]

## Parameters

  -cm[--command]: The command you want tweetStats to execute.

  -db[--database]: the name of the MongoDB database to use.

  -coll[--collection]: the name of the collection to use.

  [-regen[--regenerate]]: (True/False) Boolean indicating whether you would like the results to be recalculated. Default: True.

  [-lim[--limit]]: (Int) Number of results to return. Default: 10
