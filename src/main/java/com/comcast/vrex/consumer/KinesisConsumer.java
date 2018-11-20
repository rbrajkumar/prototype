package com.comcast.vrex.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.comcast.vrex.util.StreamUtil;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

@Service
public class KinesisConsumer {
	
	public void readStream(String streamName, String listeningRegion, String consumerName) {
		KinesisAsyncClient client = StreamUtil.createKinesisClient(listeningRegion);

        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder().streamName(streamName).build();
        DescribeStreamResponse info = client.describeStream(describeStreamRequest).join();
        StreamDescription streamDescription = info.streamDescription();
        String streamARN = streamDescription.streamARN();
        logger.info("Started listening Stream Name - " + streamName + ", ARN - " + streamARN);

        String consumerARN = StreamUtil.getConsumerARN(client, streamARN, consumerName);
        if(null == consumerARN) {
            RegisterStreamConsumerResponse registered = StreamUtil.registerConsumer(client, streamARN, consumerName);
            consumerARN = registered.consumer().consumerARN();
            logger.info("Started registering to consumer - " + consumerName + ", ARN - " + consumerARN);
        }

        List<CompletableFuture<Void>> results = new ArrayList<CompletableFuture<Void>>();
        List<Shard> shards = streamDescription.shards();
        for(Shard shard: shards) {
            SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                    .consumerARN(consumerARN)
                    .shardId(shard.shardId())
                    .startingPosition(s -> s.type(ShardIteratorType.LATEST))
                    .build();

            CompletableFuture<Void> result = callSubscribeToShardWithVisitor(client, request);
            results.add(result);
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                results.toArray(new CompletableFuture[results.size()])
        );

        allFutures.join();
	}
	
	private CompletableFuture<Void> callSubscribeToShardWithVisitor(KinesisAsyncClient client, SubscribeToShardRequest request) {
        SubscribeToShardResponseHandler.Visitor visitor = new SubscribeToShardResponseHandler.Visitor() {
            @Override
            public void visit(SubscribeToShardEvent event) {
            	logger.info("Received subscribe to shard event " + event);
                //new ShardProcessor(dynamoDBClient, event).doReporting();
                //handler.process(event);
            }
        };
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                .builder()
                .onError(t -> logger.info("Error during stream - " + t.getMessage()))
                .subscriber(visitor)
                .build();
        return client.subscribeToShard(request, responseHandler);
    }
	
	private static Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);
}