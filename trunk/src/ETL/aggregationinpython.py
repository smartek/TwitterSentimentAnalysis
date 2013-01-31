import csv
from collections import Counter
from datetime import datetime

class mapreduce:
    def messagecountforlatitude(self):
        print "latitude"
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f")
        reader = csv.reader(open("/home/savitha/Downloads/tweetoutputs/geodetailswithmessageid","rb"), delimiter="|")
        latitude = [row[1] for row in reader]
        for (k,v) in Counter(latitude).iteritems():
            if k=="0.0":
                print "%s appears %d times" % (k, v)
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f") 
    
    def messagecountfordate(self):
        print "date"
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f")
        reader = csv.reader(open("/home/savitha/Downloads/tweetoutputs/messagedetails","rb"), delimiter="|")
        date = [row[1] for row in reader]
        for (k,v) in Counter(date).iteritems():
            if k=="20121117":
                print "%s appears %d times" % (k, v)
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f") 
    
    def messagecountfortrend(self):
        print "trend"
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f")
        reader = csv.reader(open("/home/savitha/Downloads/tweetoutputs/messagedetails","rb"), delimiter="|")
        trend = [row[2] for row in reader]
        for (k,v) in Counter(trend).iteritems():
            if k=="1":
                print "%s appears %d times" % (k, v)
        now = datetime.now()
        print now.strftime("%H:%M:%S.%f") 
        
if  __name__ =='__main__':
    the_mapreduce = mapreduce()        
    the_mapreduce.messagecountforlatitude()
    the_mapreduce.messagecountfordate()
    the_mapreduce.messagecountfortrend()
