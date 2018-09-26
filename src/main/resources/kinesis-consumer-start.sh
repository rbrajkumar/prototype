#!/bin/bash

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=us-west-2

chmod 777 vrex-context-feed-w2.jar
java -jar ./vrex-context-feed-w2.jar > kinesis-log.log 2>&1 &