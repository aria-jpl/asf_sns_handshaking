from __future__ import print_function

import json, requests, urllib2, ssl
from string import Template

grq_url = "http://datasets.grfn.hysds.net:9200"

print('Loading function')

def construct_query(product_id):
    json_str = '{"query": {"bool": {"must": [{"term": {"_id": "$id"}}]}}}'
    #update_str = 
    query_temp = Template(json_str)
    query = query_temp.substitute({"id":product_id})
    return query

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    key = event['ProductKey']
    print("From SNS product key: " + key)
    info = key.split('/')
    product_id = ''
    
    for i in info:
        if i.endswith(".zip"):
            position = i.index('.zip')
            product_id = i[:position]
            
    query = construct_query(product_id)
    #query ES
    r = requests.post('%s/_search?' % (grq_url), json.dumps(query))
    result = r.text
    return result


