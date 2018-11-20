package com.comcast.vrex.util;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;

public class StreamUtil {

	private static Logger logger = LoggerFactory.getLogger(StreamUtil.class);
	
	public static KinesisAsyncClient createKinesisClient(String listeningRegion) {
        logger.info("Initialing kinesis client in region - " + Region.of(listeningRegion));
        return KinesisAsyncClient.builder()
                .region(Region.of(listeningRegion))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(5))
                .maxConcurrency(100)
                .maxPendingConnectionAcquires(10_000))
                .build();
	}
	
	public static String getConsumerARN(KinesisAsyncClient client, String streamARN, String consumerName) {
		ListStreamConsumersRequest listStreamConsumersRequest = ListStreamConsumersRequest.builder().streamARN(streamARN).build();
        ListStreamConsumersResponse consumerList = client.listStreamConsumers(listStreamConsumersRequest).join();
        List<Consumer> consumers = consumerList.consumers();
        String consumerARN = null;
        for(Consumer consumer: consumers) {
            if(consumer.consumerName().equalsIgnoreCase(consumerName)) {
                consumerARN = consumer.consumerARN();
                logger.info("Registered to consumer - " + consumerName + ", ARN - " + consumerARN);
                break;
            }
        }
		return consumerARN;
	}

	public static RegisterStreamConsumerResponse registerConsumer(KinesisAsyncClient client, String streamARN,
																	String consumerName) {
		RegisterStreamConsumerRequest register = RegisterStreamConsumerRequest.builder()
                .consumerName(consumerName).streamARN(streamARN).build();
        return client.registerStreamConsumer(register).join();
	}
}
