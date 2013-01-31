import solr

s = solr.SolrConnection('http://localhost:8983/solr')

pos_file = open("../data/pos_mod.txt", "r")            
line = pos_file.readline()
while line:
    word = line.strip()
    print word
    try:
        s.add(id=word, title=word, cat='positive')
    except ValueError:
        print word + ' error'
    line = pos_file.readline()
    
neg_file = open("../data/neg_mod.txt", "r")            
line = neg_file.readline()
while line:
    word = line.strip()
    print word
    try:
        s.add(id=word, title=word, cat='negative')
    except ValueError:
        print word + ' error'
    line = neg_file.readline()

s.commit()
    

