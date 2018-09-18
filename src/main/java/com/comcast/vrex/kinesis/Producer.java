package com.comcast.vrex.kinesis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.comcast.vrex.kinesis.common.Utils;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class Producer {
	
	public static void main(String[] args) {
		KinesisProducer kinesisProducer = createKinesisProducer();
		try {
			List<String> lines = Utils.readLinesFromFile("/Users/rradad026/Downloads/logdata/src/main/resources/xre_events.log");
			insert(kinesisProducer, lines);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void insert(KinesisProducer kinesisProducer, List<String> lines) {
		
		final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
			@Override
			public void onSuccess(UserRecordResult result) {
				System.out.println("Success - " + result.getSequenceNumber());
			}

			@Override
			public void onFailure(Throwable t) {
				if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(
                            ((UserRecordFailedException) t).getResult().getAttempts());
                    System.out.println(String.format(
                            "Record failed to put - %s : %s",
                            last.getErrorCode(), last.getErrorMessage()));
                }
				System.out.println("Exception during put" + t.getMessage());
                System.exit(1);
			}
		};
		
		for(String line : lines) {
			byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
			ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
					"vrex-test2",
					Utils.randomExplicitHashKey(),
					ByteBuffer.wrap(bytes));
			Futures.addCallback(f, callback);
		}
		System.out.println("Done!");
	}

	private static KinesisProducer createKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                // Longer timeout for slower connections
                .setRequestTimeout(60000)
                // Longer buffered time for more aggregation
                .setRecordMaxBufferedTime(15000)
                // AWS region
                .setRegion("us-east-2");
        return new KinesisProducer(config);
    }
}
