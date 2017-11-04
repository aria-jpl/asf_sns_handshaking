from __future__ import print_function

import json, requests, urllib2, ssl
from string import Template

grq_url = "http://datasets.grfn.hysds.net:9200"

print('Loading function')

authorized_ips = """
35.200.100.101 # My VPN endpoint
37.192.125.17  # Build agent 1
37.192.125.18  # Build agent 2
37.192.125.19  # Build agent 3
38.192.125.20  # Build agent 4
"""
MOZART_URL = 'https://100.64.134.67'
target_server = "http://jobs.grfn.hysds.net:8878"

def is_authorized_ip(ip):
    ips = set()
    for x in authorized_ips.strip().split("\n"):
        ips.add(x.split('#')[0].strip())

    return ip in ips

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
    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True,indent=4, separators=(',', ': ')))
    r = requests.post(job_submit_url, params=params)
    if r.status_code != 200:
        r.raise_for_status()
    result = r.json()
    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] == True:
            job_id = result['result']
            print 'submitted create_aoi:%s job: %s job_id: %s' % (release, job_id)
        else:
            print 'job not submitted successfully: %s' % result
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)

def lambda_handler(event, context):
    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event, indent=2))

    product = event['ProductName']
    print("From SNS product key: " + product)
    
    #submit mozart jobs to update ES
    job_type = "job-cmr_ingest_update"
    job_release = "release-20171103"
    job_params = json.dumps(event) #pass the whole SNS message
    tag = "asf_delivered"
    
    submit_job(job_type, job_release, product, tag, job_params)
