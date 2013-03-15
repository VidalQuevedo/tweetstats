database = 'public_sphere'
collection =  'tweets'

from pymongo import MongoClient
connection = MongoClient()
db = connection[database]


# MapReduce
from bson.code import Code

def map_field(field, field_item):
  map = Code( "function () {"
              "  this." + field + ".forEach(function(z){"
              "    emit(z."+ field_item +".toLowerCase(), 1);"
              "  });"
              "}")  
  return map

def reduce_field():
  reduce = Code("function (key, values) {"
              "  var total = 0;"
              "  for (var i=0; i < values.length; i++) {"
              "    total+= values[i];"
              "  }"
              "  return total;"
                "}")
  return reduce



# --- About set --- #
#get total num of tweets in set
def getTotalNumTweets(db, collection):
  total_num_tweets = db[collection].find().count()
  print "Total num tweets: " + str(total_num_tweets)






#avg num of tweet per day/hour/min/sec

#get start and end collection times

# --- tweets --- #

#get most retweeted tweet in set

#get tweet with highest retweet count

#--- users --- #

#get total number of users

#get users with most followers

#get most mentioned twitter user
def getMostMentionedUsers(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    map = map_field('entities.user_mentions', 'screen_name')
    reduce = reduce_field()
    result = db[collection].map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]  
  print "\n\nMost mentioned users: \n"
  for doc in result.find().limit(limit).sort('value', -1):
    print doc


#get user that tweeted the most in set

#--- hashtags --- #

#get most used hashtags
def getMostUsedHashtags(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    map = map_field('entities.hashtags', 'text')
    reduce = reduce_field()
    result = db[collection].map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "\n\nMost used hastags: \n"
  for doc in result.find().limit(limit).sort('value', -1):
    print doc


#--- links ---#
# get most linked-to urls
def getMostLinkedToUrls(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    map = map_field('entities.urls', 'expanded_url')
    reduce = reduce_field()
    result = db.tweets.map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "\n\nMost linked-to urls: \n"
  for doc in result.find().limit(limit).sort('value', -1):
    print doc


### RUN STUFF ###
#getTotalNumTweets(db, 'tweets')
#getMostUsedHashtags(db, 'tweets', 'most_used_hashtags', 25)
#getMostMentionedUsers(db, 'tweets', 'most_mentioned_users', 25)
getMostLinkedToUrls(db, 'tweets', 'most_linked_to_urls', 100)