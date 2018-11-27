package com.comcast.vrex.consumer;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.comcast.vrex.util.DBUtil;
import com.comcast.vrex.util.StreamUtil;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
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
	
	public void readStream() {
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

        List<Shard> shards = streamDescription.shards();
        for(Shard shard: shards) {
            SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                    .consumerARN(consumerARN)
                    .shardId(shard.shardId())
                    .startingPosition(s -> s.type(ShardIteratorType.LATEST))
                    .build();

            keepSubscribeToShard(getResponseHandler(), request);
        }
        
        logger.info("started listening to stream " + streamName + " as consumer " + consumerName);
	}
	
	void keepSubscribeToShard(SubscribeToShardResponseHandler responseHandler, SubscribeToShardRequest request) {
		ScheduledExecutorService execService = Executors.newScheduledThreadPool(2);
		KinesisAsyncClient client = StreamUtil.createKinesisClient(listeningRegion);
		execService.scheduleAtFixedRate(()->{
			try {
				client.subscribeToShard(request, responseHandler);
			} catch (Exception e) {
				logger.debug("Error during stream listening - " + e.getMessage());
				client.close();
				execService.shutdownNow();
				try {
					Thread.sleep(500);
				} catch (Exception e1) {
					e1.printStackTrace();
				} finally {
					keepSubscribeToShard(responseHandler, request);
				}
			}
		}, 0, 4L, TimeUnit.MINUTES);
	}
	
	private SubscribeToShardResponseHandler getResponseHandler() {
		DynamoDbAsyncClient dbClient = DBUtil.createDynamoDBClient(dbRegion);
		StreamEventProcessor processor = new StreamEventProcessor(dbClient, dbTable);
		SubscribeToShardResponseHandler.Visitor visitor = new SubscribeToShardResponseHandler.Visitor() {
            @Override
            public void visit(SubscribeToShardEvent event) {
            	logger.info("Received subscribe to shard event " + event);
            	processor.processEvent(event);
            }
        };
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                .builder()
                .publisherTransformer(p -> p.filter(e -> e instanceof SubscribeToShardEvent))
                //.onError(t -> logger.info("Error during stream - " + t.getMessage()))
                .subscriber(visitor)
                .build();
        return responseHandler;
    }
	
	@Value("${xre.kinesis.region}")
    private String listeningRegion;
	
	@Value("${xre.kinesis.name}")
    private String streamName;
	
	@Value("${xre.kinesis.fanout-consumer}")
    private String consumerName;
	
	@Value("${dynamodb.region}")
    private String dbRegion;
	
	@Value("${dynamodb.table}")
    private String dbTable;
	
	private static Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);
}