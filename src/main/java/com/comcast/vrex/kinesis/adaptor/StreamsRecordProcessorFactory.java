package com.comcast.vrex.kinesis.adaptor;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class StreamsRecordProcessorFactory implements IRecordProcessorFactory {

    private final String tableName;

    public StreamsRecordProcessorFactory(AWSCredentialsProvider dynamoDBCredentials, String dynamoDBEndpoint,
        String serviceName, String tableName) {
        this.tableName = tableName;
    }

    @Override
    public IRecordProcessor createProcessor() {
    	AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
        return new StreamsRecordProcessor(dynamoDBClient, tableName);
    }

}