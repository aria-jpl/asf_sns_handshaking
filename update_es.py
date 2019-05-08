#!/usr/bin/env python
"""
This script is for stamping docs in ES associated with products
delivered to ASF with the delivery time to ASF and ASF's ingestion time
"""
import elasticsearch
import logging
import os
import json
import requests
from hysds.celery import app

# Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)
ES_URL = app.conf.get("GRQ_ES_URL", "http://localhost:9200")
ES = elasticsearch.Elasticsearch(ES_URL)


def get_document(prod_id, index, doc_type):
    result = ES.get(index=index, doc_type=doc_type, id=prod_id)
    if result is not None:
        return result.get('_source')
    raise ValueError("Record not found for id {}".format(prod_id))


def add_doc_to_publish(prod_id, catalog_index, doc_type, es_doc):
    """
    Add a half orbit to ES
    :param hf_orbit_id:
    :param es_doc:
    :return: True if indexing was successful, else return False
    """
    result = ES.index(index="{}-released".format(catalog_index), doc_type=doc_type, body=es_doc, id=prod_id)
    if str(result["created"]) == "True":
        return True
    else:
        return False


def deliver_to_aria_products(prod_id, index, doc_type):
    """
    Copy over document from Catalog index to Publish Index.
    :param prod_id:
    :param index:
    :param type:
    :return:
    """
    doc = get_document(prod_id, index=index, doc_type=doc_type)
    if add_doc_to_publish(prod_id, catalog_index=index, doc_type=doc_type , es_doc=doc) is True:
        logging.info("{} delivered to aria-products.".format(prod_id))
    else:
        logging.info("{} FAILED to deliver to aria-products.")


def get_url_index_type_id(_id):
    """
    Returns the ES URL, index, doctype, and id of the given product
    :param _id:
    :return:
    """

    es_url = app.conf.GRQ_ES_URL
    es_index = "grq"
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
    return es_url, _index, _type, _id


def update_document(_id, delivery_time, ingest_time, delivery_status, product_tagging):
    """
    Update the ES document with new information
    :param _id: id of product delivered to ASF
    :param delivery_time: delivery time to ASF to stamp to delivered product
    :param ingest_time: ingestion time to ASF to stamp to delivered product
    :param delivery_status: status of delivery to stamp to delivered product
    :param product_tagging:
    :return:
    """
    '''
    
    Note: borrowed from user_tags
    @param product_id - 
    @param delivery_time - 
    '''

    new_doc = {
        "doc": {
            "daac_delivery_timestamp": delivery_time,
            "daac_ingest_timestamp": ingest_time,
            "daac_delivery_status": delivery_status
        },
        "doc_as_upsert": True
        }

    if product_tagging:
        new_doc["doc"]["metadata.tags"] = "daac_delivered"

    url = "{0}/{1}/{2}/{3}/_update".format(*get_url_index_type_id(_id))
    print "Updating: {0} with {1}".format(url, json.dumps(new_doc))
    req_result = requests.post(url, data=json.dumps(new_doc))
    if req_result.raise_for_status() is None:
        return True
    else:
        print req_result.raise_for_status()
        return False


if __name__ == "__main__":
    # Main program to update ES

    with open('_context.json') as f:
        ctx = json.load(f)

    # get SNS message from ASF
    sns = ctx['sns_message']
    product_tagging = ctx["product_tagging"]

    # get product id
    status = sns['Status']
    product_id = sns['ProductName']
    delivery_time = sns['DeliveryTime']
    ingest_time = sns['IngestTime']
    if status == "failure":
        status = sns['ErrorCode']
    if update_document(product_id, delivery_time, ingest_time, status, product_tagging):
        print "Successfully updated ES document"
    # temporarily removing check of successful product delivery to ASF
    es_url, _index, _type, _id = get_url_index_type_id(product_id)
    deliver_to_aria_products(product_id, index=_index, doc_type=_type)
