package com.comcast.vrex.kinesis;

import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.comcast.vrex.kinesis.consume.VrexContextFeedProcessorFactory;

public class Consumer {
	private static ProfileCredentialsProvider credentialsProvider;
	
	private static void init(){
		java.security.Security.setProperty("networkaddress.cache.ttl", "60");
		credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }
	}
	
	public static void main(String[] args) {
		//init();
		
		try{
			comsume();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void comsume() throws Exception {
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration("vrex-context-feed",
                        "vrex-test2",
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        kinesisClientLibConfiguration.withRegionName("us-east-2");
        
        IRecordProcessorFactory recordProcessorFactory = new VrexContextFeedProcessorFactory();
        Worker worker = new Worker.Builder()
        		.config(kinesisClientLibConfiguration)
        		.recordProcessorFactory(recordProcessorFactory)
        		.build();
        worker.run();
	}
}
