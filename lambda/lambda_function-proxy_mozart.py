from __future__ import print_function

import json, requests, urllib2, ssl


print('Loading function')

authorized_ips = """
35.200.100.101 # My VPN endpoint
37.192.125.17  # Build agent 1
37.192.125.18  # Build agent 2
37.192.125.19  # Build agent 3
38.192.125.20  # Build agent 4
"""

target_server = "http://jobs.grfn.hysds.net:8878"

def is_authorized_ip(ip):
    ips = set()
    for x in authorized_ips.strip().split("\n"):
        ips.add(x.split('#')[0].strip())

    return ip in ips

def lambda_handler(event, context):
    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event, indent=2))

    #if not is_authorized_ip(event['requestContext']['identity']['sourceIp']):
    #    return { 'statusCode': 403, 'body': 'IP Deny' }

    session_attributes = {}
    url = target_server + event['path']
    method = event['httpMethod']
    headers = {}
    copy_headers = ('Accept', 'Content-Type')
    event_headers = {} if event.get('headers', None) is None else event['headers']
    for h in ('Accept', 'Content-Type'):
        if h in event_headers: headers[h] = event['headers'][h]
    if method == "GET": r = requests.get(url, headers=headers)
    elif method == "POST": r = requests.post(url, headers=headers, data=event['body'])
    else: raise(NotImplementedError("Unhandled HTTP method: %s" % method))

    resp_headers = {}
    for h in ('Date', 'Content-Type', 'Connection', 'Content-Length', 'Server'):
        if h in r.headers: resp_headers[h] = r.headers[h]
    print("response headers: %s" % json.dumps(resp_headers, indent=2))

    return {
        'statusCode': r.status_code,
        'headers': resp_headers,
        'body': r.text,
    }
