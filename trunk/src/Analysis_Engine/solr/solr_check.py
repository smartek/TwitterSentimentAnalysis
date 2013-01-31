import solr

config = {}
execfile("../settings.conf", config)

solr_url =  config["solr_url"]

s = solr.SolrConnection(solr_url)
response = s.query('title:beautiful')
print response.results[0]['cat'][0]