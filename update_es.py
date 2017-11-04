import json, requests
import logging
import sys, os, datetime
import hysds_commons.request_utils
import hysds_commons.metadata_rest_utils
import osaka.main
from string import Template
from hysds.celery import app

#Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

def getURLIndexTypeID(product_id):
   '''
   Returns the ES URL, index, doctype, and id of the given product
   @param product_id - id of the product to search for 
   '''
   es_url = app.conf.GRQ_ES_URL
   es_index = "grq"
   _id = product_id
   query = {
        "query": {
            "bool": {
                "must": [
                    { "term": { "_id": _id  }  }
                ]
            }
        }
   }
   url = "%s/%s/_search/" % (es_url, es_index)
   data=json.dumps(query,indent=2)
   print("Posting ES search: {0} with {1}".format(url,data))
   r = requests.post(url,data=data)
   r.raise_for_status()
   print("Got: {0}".format(r.json()))
   result = r.json()
   if len(result["hits"]["hits"]) == 0:
       raise Exception("Product not found in ES index: {0}".format(product_id))
   elif len(result["hits"]["hits"])  > 1:
       raise Exception("Product found multiple times: {0}".format(product_id))
   return (es_url,result["hits"]["hits"][0]["_index"],result["hits"]["hits"][0]["_type"],result["hits"]["hits"][0]["_id"])

def update_document(product_id, delivery_time, ingest_time):
   '''
   Update the ES document with new information
   Note: borrowed from user_tags
   @param product_id - id of product delivered to ASF 
   @param delivery_time - delivery time to ASF to stamp to delivered product
   '''

   new_doc = {
      "doc": {
         "ASF_delivery_time": delivery_time,
         "ASF_ingestion_time": ingestion_time
         },
      "doc_as_upsert": true
      }

   #url = "{0}/{1}/{2}/{3}/_update".format(*getURLIndexTypeID(product_id))
   url = "{0}/{1}/grq_v1.1.2_s1-ifg_test/{3}/_update".format(*getURLIndexTypeID(product_id))
   print("Updating: {0} with {1}".format(url,new_doc))
   r = requests.post(url, data=json.dumps(new_doc))
   if r.raise_for_status() == None:
      return True
   else:
      return False


if __name__ == "__main__":
   '''
   Main program to update ES
   '''

   with open('_context.json') as f:
      ctx = json.load(f)

   #get SNS message from ASF
   sns = ctx['sns_message']
   #get product id
   status = sns['Status']
   product_id = sns['ProductName']
   if status == "success":
      delivery_time = sns['DeliveryTime']
      ingest_time = sns['IngestTime'] 
      if(update_document(product_id, delivery_time, ingest_time)):
          print "Successfully updated ES document"
   
