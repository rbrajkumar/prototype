package com.comcast.vrex.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

public class DBUtil {
	
	public DynamoDbStreamsAsyncClient createDynamoDbStreamsAsyncClient(String listeningRegion) {
		return DynamoDbStreamsAsyncClient.create();
	}
	
	public static DynamoDbAsyncClient createDynamoDBClient(String listeningRegion) {
		logger.debug("creating new DB client with region " + listeningRegion);
		return DynamoDbAsyncClient.builder().region(Region.of(listeningRegion)).build();
	}
	
	public static DynamoDbAsyncClient createDefaultClient() {
		return DynamoDbAsyncClient.create();
	}
	
	public static DynamoDbAsyncClient createDefaultProfileDynamoDBClient(String listeningRegion) {
		return DynamoDbAsyncClient.builder()
				.region(Region.of(listeningRegion))
				.credentialsProvider(ProfileCredentialsProvider.builder().profileName("default").build())
				.build();
	}
	
	private static Logger logger = LoggerFactory.getLogger(DBUtil.class);
}
