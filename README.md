# Ingestr

Ingestr is a microservice that consumes Ethereum blocks for enqueing into SNS and storage in S3.

### Requirements
* An Ethereum node
* An AWS IAM role with permission to read/write from an S3 bucket, and an SNS topic
