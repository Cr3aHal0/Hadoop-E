#!/usr/bin/python
# -*- coding: utf-8 -*-
from org.apache.pig.scripting import *
P = Pig.compile("""
previous_pagerank = 
    LOAD '$docs_in' 
    USING PigStorage('\t') 
    AS ( url: chararray, pagerank: float, links:{ link: ( url: chararray ) } );

outbound_pagerank = 
    FOREACH previous_pagerank 
    GENERATE 
        pagerank / COUNT ( links ) AS pagerank, 
        FLATTEN ( links ) AS to_url;

new_pagerank = 
    FOREACH 
        ( COGROUP outbound_pagerank BY to_url, previous_pagerank BY url INNER )
    GENERATE 
        group AS url, 
        ( 1 - $d ) + $d * SUM ( outbound_pagerank.pagerank ) AS pagerank, 
        FLATTEN ( previous_pagerank.links ) AS links;
        
STORE new_pagerank 
    INTO '$docs_out'
    USING PigStorage('\t');
""")

params = { 'd': '0.85', 'docs_in': 'data/in' }

for i in range(10):
   out = "data/out/pagerank_data_" + str(i + 1)
   params["docs_out"] = out
   Pig.fs("rm -r " + out)
   stats = P.bind(params).runSingle()
   if not stats.isSuccessful():
      raise 'failed'
   params["docs_in"] = out
 
