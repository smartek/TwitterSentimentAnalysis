import web
import pickle
import solr
from classifier import baseline_classifier

urls = (
  '/classify/', 'classify',
  '/positive/','positive',
  '/negative/','negative')

app = web.application(urls, globals())

class classify:
    def GET(self):        
        return config["status_405"]
    
    def POST(self):
        try:
            i = web.input()
            data = web.data()
            print data
            tweets = [s.strip() for s in data[1:-1].split(',')]
            dict_tweet = {}
            dict_tweet[0] = tweets
            bc = baseline_classifier.BaselineClassifier(dict_tweet)
            bc.classifyBySolr()
            result = bc.getOutput()
            return result
        except:
            return config["input_error"]

class positive:
    def GET(self):        
        return config["status_405"]
    
    def POST(self):
        try:
            i = web.input()
            data = web.data()
            words = [s.strip() for s in data[1:-1].split(',')]
            try:
                s = solr.SolrConnection(config["solr_url"])
                for word in words:
                    print word
                    s.add(id=word, title=word, cat='positive')
                s.commit()
            except:
                print sys.exc_info()     
            return config["words_updated"]
        except:
            return config["input_error"]

class negative:
    def GET(self):        
        return status_405
    
    def POST(self):
        try:
            i = web.input()
            data = web.data()
            words = [s.strip() for s in data[1:-1].split(',')]
            try:
                s = solr.SolrConnection(config["solr_url"])
                for word in words:
                    print word
                    s.add(id=word, title=word, cat='negative')
                s.commit()
            except:
                print sys.exc_info()     
            return config["words_updated"]
        except:
            return status_405

if __name__ == "__main__":
    config = {}
    execfile("../settings.conf", config)  
    app.run()