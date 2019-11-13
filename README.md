# Ingestr

Ingestr is a Go microservice that consumes Ethereum blocks for enqueing into SNS and storage in S3.

![My image](https://prettymuchbryce.s3-us-west-1.amazonaws.com/ingestr.png)
  
### Description

Ingestr pulls new blocks out of an Ethereum node (Geth, Parity, Infura, etc), and publishes the block number to SNS, as well as caches it in a gzipped form in S3.

Ingestr is useful for systems that need to:
  * Actively ingest new Ethereum blocks to perform some processing
  * Backfill historical blocks
  * Want to use the same code path to do both of these things
  
Ingestr requires redis to store some information about the blocks it is working on. It is safe to run multiple ingestr instances in parallel with each other (if you're into that sort of thing), however your biggest bottleneck is likely to be throughput from your Ethereum node.

For this reason, Ingestr caches all blocks in S3 so that on subsequent runs, blocks can be fetched from there instead.

Although Ingestr enqueues block numbers into SNS when work is finished, it's expected that downstream consumers of these events will fetch (and gunzip) the blocks directly from S3.
  
### Configuration

Please see the [Environment Variables](https://github.com/prettymuchbryce/ingestr/blob/master/.env).

### Requirements
* An Ethereum node (or Infura, or whatever)
* A redis instance
* An AWS IAM role with permission to read/write from an S3 bucket, and an SNS topic
