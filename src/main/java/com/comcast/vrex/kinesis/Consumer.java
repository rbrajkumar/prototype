package com.comcast.vrex.kinesis;

import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.comcast.vrex.kinesis.consume.VrexContextFeedProcessorFactory;

public class Consumer {
	
	public static void main(String[] args) {
		try{
			comsume();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void comsume() throws Exception {
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                "vrex-feed-consumer",
                "vrex-test2",
                new DefaultAWSCredentialsProviderChain(),
                workerId
        );
        config.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        config.withIdleTimeBetweenReadsInMillis(200);
        config.withRegionName("us-east-2");

        final IRecordProcessorFactory recordProcessorFactory = new VrexContextFeedProcessorFactory();

        final Worker worker = new Worker.Builder()
                .config(config)
                .recordProcessorFactory(recordProcessorFactory)
                .build();

        worker.run();
	}
}
