from twitter import get_twitter_data
from classifier import baseline_classifier

keyword = 'iphone'
time = 'today'

twitterData = get_twitter_data.TwitterData()
tweets = twitterData.getTwitterData(keyword, time)

print type(tweets)
print tweets

#tweets = {}
#twts = []
#twts.append("@Marinda_Diaz: I swear I'm the only person on this earth who doesn't have an iPhone ")
#twts.append("@_Jay_Cartwright: I got the NEW limited edition see through iPhone 5 for Christmas.")
#tweets[0]=twts

#Baseline Classifier
#bc = baseline_classifier.BaselineClassifier(tweets)
#bc.classifyBySolr()
#bc.classify()
#result = bc.getOutput()

#print result
        
    
