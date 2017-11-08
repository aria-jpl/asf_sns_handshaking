## asf_sns_handshaking
When products are delivered to ASF, we recieve an acknowledgement SNS message from ASF. This message reports status of the delivery (success or failure). If the delivery was a success, we want to stamp the product in ES with the ASF ingest time and delivery time.


This repo contains:
* Updating ES script
* Lambda function code for ASF SNS handshaking (triggers off of the SNS channel)
