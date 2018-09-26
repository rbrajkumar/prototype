package com.comcast.vrex.kinesis.consume;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class VrexContextFeedProcessorFactory implements IRecordProcessorFactory {

	@Override
	public IRecordProcessor createProcessor() {
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		return new LogFeedProcessor(dynamoDBClient);
	}

}
