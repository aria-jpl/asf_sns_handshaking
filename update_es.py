#!/usr/bin/env python

'''
This script is for stamping docs in ES associated with products
delivered to ASF with the delivery time to ASF and ASF's ingestion time
'''

import logging
import os
import json
import requests
from hysds.celery import app

#Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

def get_url_index_type_id(_id):
    '''
    Returns the ES URL, index, doctype, and id of the given product
    @param product_id - id of the product to search for
    '''
    es_url = app.conf.GRQ_ES_URL
    es_index = "grq"
    #_id = product_id
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": _id}}
                ]
            }
        }
    }
    url = "%s/%s/_search/" % (es_url, es_index)
    data = json.dumps(query, indent=2)
    print "Posting ES search: {0} with {1}".format(url, data)
    req_result = requests.post(url, data=data)
    req_result.raise_for_status()
    print "Got: {0}".format(req_result.json())
    result = req_result.json()
    if len(result["hits"]["hits"]) == 0:
        raise Exception("Product not found in ES index: {0}".format(_id))
    elif len(result["hits"]["hits"]) > 1:
        raise Exception("Product found multiple times: {0}".format(_id))
    _index = result["hits"]["hits"][0]["_index"]
    _type = result["hits"]["hits"][0]["_type"]
    _id = result["hits"]["hits"][0]["_id"]
    return (es_url, _index, _type, _id)

def update_document(_id, asf_delivery_time, asf_ingest_time):
    '''
    Update the ES document with new information
    Note: borrowed from user_tags
    @param product_id - id of product delivered to ASF
    @param delivery_time - delivery time to ASF to stamp to delivered product
    '''

    new_doc = {
        "doc": {
            "ASF_delivery_time": asf_delivery_time,
            "ASF_ingestion_time": asf_ingest_time
        },
        "doc_as_upsert": True
        }

    #url = "{0}/{1}/{2}/{3}/_update".format(*get_url_index_type_id(product_id))
    #just for testing purposes. Created grq_v1.1.2_s1-ifg_test index. It is a copy of the ifg index.
    url = "{0}/{1}/grq_v1.1.2_s1-ifg_test/{3}/_update".format(*get_url_index_type_id(_id))
    print "Updating: {0} with {1}".format(url, json.dumps(new_doc))
    req_result = requests.post(url, data=json.dumps(new_doc))
    if req_result.raise_for_status() is  None:
        return True
    else:
        print req_result.raise_for_status()
        return False


if __name__ == "__main__":
    #Main program to update ES

    with open('_context.json') as f:
        CTX = json.load(f)

    #get SNS message from ASF
    SNS = CTX['sns_message']
    #get product id
    STATUS = SNS['Status']
    PRODUCT_ID = SNS['ProductName']
    if STATUS == "success":
        DELIVERY_TIME = SNS['DeliveryTime']
        INGEST_TIME = SNS['IngestTime']
        if update_document(PRODUCT_ID, DELIVERY_TIME, INGEST_TIME):
            print "Successfully updated ES document"
