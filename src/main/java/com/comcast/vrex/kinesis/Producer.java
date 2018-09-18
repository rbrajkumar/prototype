package com.comcast.vrex.kinesis;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.comcast.vrex.common.Utils;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
    
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());    
    private static final int DATA_SIZE = 128;   
    private static final int SECONDS_TO_RUN = 5;    
    private static final int RECORDS_PER_SECOND = 4;  
    
    public static final String STREAM_NAME = "vrex-test";   
    public static final String REGION = "us-east-2";

    public static KinesisProducer getKinesisProducer() {
        // KinesisProducerConfiguration config =
        //     KinesisProducerConfiguration.fromPropertiesFile("default_config.properties");
        
    	KinesisProducerConfiguration config = new KinesisProducerConfiguration();        
        config.setRegion(REGION);
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());        
        config.setMaxConnections(1);       
        config.setRequestTimeout(60000);        
        config.setRecordMaxBufferedTime(15000);
        KinesisProducer producer = new KinesisProducer(config);        
        return producer;
    }
    
    public static void main(String[] args) throws Exception {
        //process();
        insertLogstoKinesisStream(STREAM_NAME);
    }
    
    public static void insertLogstoKinesisStream(String streamName) {
    	final KinesisProducer producer = getKinesisProducer();
    	
    	PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        
        List<String> lines = new ArrayList<>();
		try {
			lines = Utils.readLinesFromFile("/work/code/tmp/2del/2468/code/LogProcess/src/main/resources/xre_events.log");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for(String line : lines) {
        	producer.addUserRecord(STREAM_NAME, lines.indexOf(line)+"", ByteBuffer.wrap(line.getBytes()));
        }
        System.out.println("Done");
    }

	private static void process() throws InterruptedException {
		final KinesisProducer producer = getKinesisProducer();
        final AtomicLong sequenceNumber = new AtomicLong(0);
        final AtomicLong completed = new AtomicLong(0);
        
        // KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(
                            ((UserRecordFailedException) t).getResult().getAttempts());
                    log.error(String.format(
                            "Record failed to put - %s : %s",
                            last.getErrorCode(), last.getErrorMessage()));
                }
                log.error("Exception during put", t);
                System.exit(1);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                completed.getAndIncrement();
            }
        };
        
        // The lines within run() are the essence of the KPL API.
        final Runnable putOneRecord = new Runnable() {
            @Override
            public void run() {
                ByteBuffer buffer = null;
				try {
					String data = Utils.generateData(sequenceNumber.get());
	                buffer = ByteBuffer.wrap(data.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
                // TIMESTAMP is our partition key
                ListenableFuture<UserRecordResult> f =
                        producer.addUserRecord(STREAM_NAME, sequenceNumber.get()+"", Utils.randomExplicitHashKey(), buffer);
                Futures.addCallback(f, callback);
            }
        };
        
        // This gives us progress updates
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long put = sequenceNumber.get();
                long total = RECORDS_PER_SECOND * SECONDS_TO_RUN;
                double putPercent = 100.0 * put / total;
                long done = completed.get();
                double donePercent = 100.0 * done / total;
                log.info(String.format(
                        "Put %d of %d so far (%.2f %%), %d have completed (%.2f %%)",
                        put, total, putPercent, done, donePercent));
            }
        }, 1, 1, TimeUnit.SECONDS);
        
        // Kick off the puts
        log.info(String.format(
                "Starting puts... will run for %d seconds at %d records per second",
                SECONDS_TO_RUN, RECORDS_PER_SECOND));
        executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, SECONDS_TO_RUN, RECORDS_PER_SECOND);
        
        // Wait for puts to finish. After this statement returns, we have
        // finished all calls to putRecord, but the records may still be
        // in-flight. We will additionally wait for all records to actually
        // finish later.
        EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS);
        
        // If you need to shutdown your application, call flushSync() first to
        // send any buffered records. This method will block until all records
        // have finished (either success or fail). There are also asynchronous
        // flush methods available.
        //
        // Records are also automatically flushed by the KPL after a while based
        // on the time limit set with Configuration.setRecordMaxBufferedTime()
        log.info("Waiting for remaining puts to finish...");
        producer.flushSync();
        log.info("All records complete.");
        
        // This kills the child process and shuts down the threads managing it.
        producer.destroy();
        log.info("Finished.");
	}

    /**
     * Executes a function N times per second for M seconds with a
     * ScheduledExecutorService. The executor is shutdown at the end. This is
     * more precise than simply using scheduleAtFixedRate.
     * 
     * @param exec
     *            Executor
     * @param task
     *            Task to perform
     * @param counter
     *            Counter used to track how many times the task has been
     *            executed
     * @param durationSeconds
     *            How many seconds to run for
     * @param ratePerSecond
     *            How many times to execute task per second
     */
    private static void executeAtTargetRate(
            final ScheduledExecutorService exec,
            final Runnable task,
            final AtomicLong counter,
            final int durationSeconds,
            final int ratePerSecond) {
        exec.scheduleWithFixedDelay(new Runnable() {
            final long startTime = System.nanoTime();

            @Override
            public void run() {
                double secondsRun = (System.nanoTime() - startTime) / 1e9;
                double targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond;
                
                while (counter.get() < targetCount) {
                    counter.getAndIncrement();
                    try {
                        task.run();
                    } catch (Exception e) {
                        log.error("Error running task", e);
                        System.exit(1);
                    }
                }
                
                if (secondsRun >= durationSeconds) {
                    exec.shutdown();
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }
}