#!/usr/bin/env python
# coding=utf-8
import os
import json
import io
import csv
import shutil
import sys
import itertools
import operator
sys.path.append ('/usr/local/hive/hive-0.8.1/lib/py')
from operator import itemgetter
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from datetime import datetime

class get_tweetdetails:
    
    def get_userdetails(self,data):
        user_datainput = [{'username':record['from_user'], 'userid':record['from_user_id']} for record in data['results']]
        return user_datainput
    
    def get_geo(self,data):
        geo_data = [{'type':record['geo']['type'],'lat':record['geo']['coordinates'][0],'long':record['geo']['coordinates'][1],'messageid':record['id']} for record in data['results'] if record['geo']]
        return geo_data
    
    def get_messages(self,data):
        message_data = [{'messageid':record['id'], 'userid':record['from_user_id'],'message':record['text'], 'createddate':record['created_at'],'trend':data['query'],} for record in data['results']]
        return message_data
    
    def get_trends(self,data):
        trend_datainput = (data['query'])
        return trend_datainput
  

class tweet_extractor:
      
    global tweet_details
    tweet_details = get_tweetdetails()
    global createddatefortrend
    origlist=[]
    complist={}
    
    def extract_userdata(self):
        inputpath ='/home/savitha/Downloads/tweetoutputs/output/Output/'
        outputpath ='/home/savitha/Downloads/tweetoutputs/userdetails'
        listing = os.listdir(inputpath)
        usertext_file = open(outputpath,"w")
        output=[]
        orglist=[]
        orgdict={'userid':'','username':''}
        for pt in listing:
            inputfilename=''.join([inputpath,pt])
            try:
                tweet_data = json.load(open(inputfilename))
            except(ValueError):
                print pt
            user_details = get_tweetdetails.get_userdetails(tweet_details,tweet_data)
            space = "\n"
            separator="|"
            for x in user_details:
                orgdict['username'] = x['username']
                orgdict['userid'] = x['userid']
                orglist.append(orgdict.copy())
            getvals = operator.itemgetter('username', 'userid')
            orglist.sort(key=getvals)
            result = []
            for k, g in itertools.groupby(orglist, getvals):
                result.append(g.next())
 
            for v in result:
                username=v['username']
                userid=v['userid']
                usertext_file.write(''.join([str(userid),str(separator).strip(),str(username).strip(),str(space)]))
        usertext_file.close()

    def extract_geodata(self):
        inputpath ='/home/savitha/Downloads/tweetoutputs/output/Output/'
        outputpath ='/home/savitha/Downloads/tweetoutputs/geodetailswithmessageid'
        listing = os.listdir(inputpath)
        geotext_file = open(outputpath,"w")
        for pt in listing:
            inputfilename=''.join([inputpath,pt])
            tweet_data = json.load(open(inputfilename))
            geo_details = get_tweetdetails.get_geo(tweet_details,tweet_data)
            space = "\n"
            separator="|"
            for x in geo_details:
                typeof=x['type']
                lat=x['lat']
                longitude=x['long']
                messageid=x['messageid']
                geotext_file.write(''.join([str(typeof).strip(),str(separator).strip(),str(lat),str(separator).strip(),str(longitude),str(separator).strip(),str(messageid),str(space)]))
        geotext_file.close()
            
    def extract_trenddata(self):
        inputpath ='/home/savitha/Downloads/tweetoutputs/output/Output/'
        outputpath ='/home/savitha/Downloads/tweetoutputs/trends'
        listing = os.listdir(inputpath)
        trenddatatext_file = open(outputpath,"w")
        space = "\n"
        separator="|"
        output = []  
        for pt in listing:
            inputfilename=''.join([inputpath,pt])
            tweet_data = json.load(open(inputfilename))
            test=(str(get_tweetdetails.get_trends(tweet_details,tweet_data))).lower()
            if test not in output:
                output.append(test) 
        for i, trend in enumerate(output):
            trenddatatext_file.write(''.join([str(i+1),str(separator).strip(),trend.lower().strip(),str(space)]))
        trenddatatext_file.close() 
                
    
    def extract_messages(self):
        inputpath ='/home/savitha/Downloads/tweetoutputs/output/Output/'
        outputpath ='/home/savitha/Downloads/tweetoutputs/messagedetails'
        origlist= []
        messageidtrenddict ={'messageid':'','message':'','userid':'','createddate':'','trend':''}
        listing = os.listdir(inputpath)
        msgtext_file = open(outputpath,"w")
        for pt in listing:
            inputfilename=''.join([inputpath,pt])
            tweet_data = json.load(open(inputfilename))
            message_details = get_tweetdetails.get_messages(tweet_details,tweet_data)
            space = "\n"
            separator="|"
            for x in message_details:
                messageidtrenddict['userid']=x['userid']
                messageidtrenddict['message']=x['message']
                t=datetime.strptime(x['createddate'],'%a, %d %b %Y %H:%M:%S +0000')
                d=t.strftime("%Y%m%d")
                messageidtrenddict['createddate']=d
                messageidtrenddict['messageid']=x['messageid']
                messageidtrenddict['trend']=x['trend'].lower()
                origlist.append(messageidtrenddict.copy())
        compfile=open("/home/savitha/Downloads/tweetoutputs/trends")
        readercomp=csv.reader(compfile,delimiter='|')
        complist={}
        complist=[{'id':row[0],'trend':row[1]} for row in readercomp]
        space = "\n"
        separator="|"
        for i in origlist:
                countFound=False
                for j in complist:
                        if i['trend']==j['trend']:
                                countFound=True
                                break
                if countFoundl==False:
                    print("Not Found"," ",j['id'])
                else:
                     msgtext_file.write(''.join([str(i['messageid']),str(separator).strip(),str(separator).strip(),str(i['createddate']).strip(),str(separator).strip(),str(j['id']).strip(),str(space)]))
#                    msgtext_file.write(''.join([str(i['messageid']),str(separator).strip(),str(i['message'].encode('utf-8')).strip(),str(separator).strip(),str(i['userid']).strip(),str(separator).strip(),str(i['createddate']).strip(),str(separator).strip(),str(j['id']).strip(),str(space)]))
        msgtext_file.close()


class mvfilestoarchive:
    
    def filestoarchive(self):
#        path ='/home/savitha/Desktop/archive/'
        path ='/home/savitha/Downloads/tweetoutputs/output/Output/'
        print time.ctime()
        files = os.listdir(path)
        files.sort()
        for f in files:
            src = path+f
#            dst = '/home/savitha/Downloads/tweetoutputs/output/Output/' 
            dst = '/home/savitha/Desktop/archive/' 
            shutil.move(src, dst)
        print time.ctime()

class importdatatohive:
    
    def datatohive(self):
        try:
            transport = TSocket.TSocket('192.168.11.72', 11000)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = ThriftHive.Client(protocol)
            transport.open()
            client.execute("CREATE EXTERNAL TABLE Trendstemp(TrendId INT,Trend STRING,date_created STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE")
            client.execute("CREATE TABLE Userdetails(UserId BIGINT,Username STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE")
            client.execute("CREATE TABLE Trends(TrendId INT,Trend STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE")
            client.execute("CREATE TABLE Geodetailswithmessageid(Geotype STRING,Latitude string,Longitude string,MessageId BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE")
            client.execute("CREATE TABLE Messagedetails(MessageId BIGINT,Message STRING,UserId BIGINT,Createddate int,Trendid int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE")
            client.execute("LOAD DATA LOCAL INPATH '/home/savitha/Downloads/tweetoutputs/userdetails' INTO TABLE Userdetailstemp")
            client.execute("LOAD DATA LOCAL INPATH '/home/savitha/Downloads/tweetoutputs/trends' INTO TABLE Trendstemp")
            client.execute("INSERT INTO TABLE Trends SELECT DISTINCT TrendId,Trend FROM Trendstemp")
            client.execute("LOAD DATA LOCAL INPATH '/home/savitha/Downloads/tweetoutputs/messagedetails' INTO TABLE Messagedetails")
            client.execute("LOAD DATA LOCAL INPATH '/home/savitha/Downloads/tweetoutputs/geodetailswithmessageid' INTO TABLE Geodetailswithmessageid")


 
            print client.fetchAll()
            transport.close()

        except Thrift.TException, tx:
            print '%s' % (tx.message)
    
if  __name__ =='__main__':
    the_tweet = tweet_extractor()   
    the_archivefile=mvfilestoarchive()
    the_importhive=importdatatohive()
    the_tweet.extract_userdata()
    the_tweet.extract_geodata()
    the_tweet.extract_trenddata()
    the_tweet.extract_messages()
#    the_archivefile.filestoarchive()
#    the_importhive.datatohive()
#    the_mapreduce.messagecountfordate()
 
