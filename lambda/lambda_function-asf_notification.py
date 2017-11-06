#!/usr/bin/env python
from __future__ import print_function

'''
This lambda function submits a job via MOZART API
to update ES doc, for book keeping purposes.
When an SNS message is recieved from ASF reporting
successful receipt and ingestion of product delivered by our system,
we want to capture this acknowledgement by stamping the product
with delivery and ingestion time.
'''

import json
import  requests

print 'Loading function'

MOZART_URL = 'https://100.64.134.67'

def submit_job(job_type, release, product_id, tag, job_params):
    '''
    submits a job to mozart
    '''
    # submit mozart job
    job_submit_url = '%s/mozart/api/v0.1/job/submit' % MOZART_URL
    params = {
        'queue': 'grfn-job_worker-small',
        'priority': '5',
        'job_name': 'job_%s-%s' % ('es_update', product_id),
        'tags': '["%s"]' % tag,
        'type': '%s:%s' % (job_type, release),
        'params': job_params,
        'enable_dedup': False

    }
    print 'submitting jobs with params:'
    print json.dumps(params, sort_keys=True, indent=4, separators=(',', ': '))
    req = requests.post(job_submit_url, params=params)
    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] is True:
            job_id = result['result']
            print 'submitted upate ES:%s job: %s job_id: %s' % (job_type, release, job_id)
        else:
            print 'job not submitted successfully: %s' % result
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)

def lambda_handler(event, context):
    '''
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    '''

    print "Got event of type: %s" % type(event)
    print "Got event: %s" % json.dumps(event, indent=2)
    print "Got context: %s"% context

    product = event['ProductName']
    print "From SNS product key: %s" % product
    #submit mozart jobs to update ES
    job_type = "job-cmr_ingest_update"
    job_release = "release-20171103"
    job_params = {'sns_message': json.dumps(event)} #pass the whole SNS message
    tag = "asf_delivered"

    submit_job(job_type, job_release, product, tag, job_params)
