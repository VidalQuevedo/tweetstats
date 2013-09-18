# MapReduce
from bson.code import Code

def map_array_field(field, field_item):
  map = Code( "function () {"
              "  this." + field + ".forEach(function(z){"
              "    emit(z."+ field_item +", 1);"
              "  });"
              "}")  
  return map

def map_field(field):
  map = Code( "function () {"
              "  emit(this." + field + ", 1);"
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

# mapreduce for retweet_count
def map_retweet_count(field):
  map = Code( "function(){"
              " emit(this." + field + ", this.retweeted_status.retweet_count);"
              "}")
  return map

def reduce_retweet_count():
  reduce = Code( "function (key, values) {"
              " var highest = Math.max.apply(Math, values);"
              " return highest;"
              "}" )
  return reduce

# --- About set --- #
#get total num of tweets in set
def getTotalNumTweets(db, collection):
  total_num_tweets = db[collection].count()
  print "- Total number of tweets: " + str(total_num_tweets)






#avg num of tweet per day/hour/min/sec

#get start and end collection times

# --- tweets --- #


# --- retweets --- #

#get total number of RTd
def getTotalNumberOfRTd(db, collection):
  count = db[collection].find({'retweeted': 'true'}).count()
  print count

#get tweets with highest retweet count
def getTweetsWithHighestRtCount(db, collection, results_collection, limit = 10, regenerate = False):
  if(regenerate):
    #db[results_collection].remove()
    # db.temp.remove()
    # for doc in db[collection].find({'retweeted_status': {'$exists' : True}}):
    #   db.temp.insert(doc)
    map = map_retweet_count('retweeted_status.id_str')
    reduce = reduce_retweet_count()
    result = db.temp.map_reduce(map, reduce, results_collection)
  else:
    result = db[results_collection]
  print "\nTweets with highest count: \b"
  for doc in result.find().limit(limit).sort('value', -1):
    print '\t - ' + doc


# --- users --- #

#get total number of users
def getTotalNumberOfUsers(db, collection, results_collection, regenerate = False):
  if (regenerate):
    db[results_collection].remove()
    for doc in db[collection].distinct('user.id_str'):
      db[results_collection].insert({'id_str': doc})
  count = db[results_collection].count()
  print "- Total number of users: " + str(count)


#get users with most followers




#get most mentioned twitter user
def getMostMentionedUsers(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_array_field('entities.user_mentions', 'screen_name')
    reduce = reduce_field()
    result = db[collection].map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]  
  print "- Most mentioned users (top " + str(limit) + "):"
  for doc in result.find().limit(limit).sort('value', -1):
    print "   -" + str(doc)


#tweets per user
def getNumberOfTweetsPerUser(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_field('user.screen_name')
    reduce = reduce_field()
    result = db[collection].map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]  
  print "- Users with most tweets (top " + str(limit) + "):"
  for doc in result.find().limit(limit).sort('value', -1):
    print "   -" + str(doc)


#--- hashtags --- #

#get most used hashtags
def getMostUsedHashtags(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_array_field('entities.hashtags', 'text')
    reduce = reduce_field()
    result = db[collection].map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "- Total number of hashtags: " + str(db[reduced_collection].count())
  print "- Most used hastags (top " + str(limit) + "):"
  for doc in result.find().limit(limit).sort('value', -1):
    print '   -' + str(doc)


#--- links ---#
# get most linked-to urls
def getMostLinkedToUrls(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_array_field('entities.urls', 'expanded_url')
    reduce = reduce_field()
    result = db.tweets.map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "- Most linked-to urls:"
  for doc in result.find().limit(limit).sort('value', -1):
    print "   -" + str(doc)

#-- media --#
# most tweeted media 
# print "\n\nMost tweeted media: \n"
# map = map_array_field('entities.media', 'type')
# reduce = reduce_field()
# result = db.tweets.map_reduce(map, reduce, "myresults")
# for doc in result.find().limit(10).sort('value', -1):
#   print doc

#--- Conversations ---#

def getMostRepliedToUsers(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_field('in_reply_to_user_id_str')
    reduce = reduce_field()
    result = db.tweets.map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "\n\nMost replied to users: \n"
  for doc in result.find().limit(limit).sort('value', -1):
    print doc

def getMostRepliedToTweets(db, collection, reduced_collection, limit = 10, run_map_reduce = False):
  if (run_map_reduce):
    db[reduced_collection].remove()
    map = map_field('in_reply_to_status_id_str')
    reduce = reduce_field()
    result = db.tweets.map_reduce(map, reduce, reduced_collection)
  else:
    result = db[reduced_collection]
  print "\n\nMost replied to tweets: \n"
  for doc in result.find().limit(limit).sort('value', -1):
    print doc

def getConversations(db, limit = 1):
  for doc in db.most_replied_to_tweets.find({'_id':{'$ne':None},'value':{'$gte': 3}}).limit(limit):
    # print doc
    print '---- \n'
    for doc2 in db.tweets.find({'id_str': str(doc['_id'])}):
      print "(Original)" + doc2['created_at'] + ' - ' + doc2['user']['screen_name'] + ': ' + doc2['text'] + '\n'
      for doc3 in db.tweets.find({'in_reply_to_status_id_str': doc['_id']}).sort('id_str', 1):
        print doc3['created_at'] + ' - ' + doc3['user']['screen_name'] + ': ' + doc3['text'] + '\n'


def getDescriptives(db, collection):
  print "Basic Descriptives from database \"" + db.name + "\", collection \"" + collection + "\":"
  getTotalNumTweets(db, collection)
  
  print '\n### Users ###'
  getTotalNumberOfUsers(db, collection, 'user_ids', False)
  getNumberOfTweetsPerUser(db, collection, 'number_of_tweets_per_user', 5, False)
  getMostMentionedUsers(db, collection, 'most_mentioned_users', 5,False)

  print '\n### Hashtags ###'
  getMostUsedHashtags(db, collection, 'most_used_hashtags',5, False)

  print '\n### Links ###'
  getMostLinkedToUrls(db, collection, 'most_linked_to_urls', 5, False)



### RUN STUFF ###

from pymongo import MongoClient

# database = 'public_sphere'
database = 'obamacare'
collection =  'tweets'

connection = MongoClient()
db = connection[database]

# getDescriptives(db, collection)
# getTotalNumberOfRTd(db, collection)
# getTweetsWithHighestRtCount(db, collection, 'retweets_with_highest_count', 25)
# getMostRepliedToUsers(db, collection, 'most_replied_to_users', 10) 
# getConversations(db, collection, 'most_replied_to_tweets', 10)
# getConversations(db, 50)
# getMostRetweeted(db, collection) #0
