import solr

s = solr.SolrConnection('http://localhost:8983/solr')

s.delete_query('title:*.*')

s.commit()
