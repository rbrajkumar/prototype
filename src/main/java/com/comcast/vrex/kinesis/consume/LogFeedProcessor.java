package com.comcast.vrex.kinesis.consume;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.comcast.vrex.kinesis.consume.analysis.BatchAnalysis;

public class LogFeedProcessor implements IRecordProcessor {

	private final AmazonDynamoDB dynamoDBClient;
	private Record prevRecord = null;
    
	public LogFeedProcessor(AmazonDynamoDB dynamoDBClient2) {
		this.dynamoDBClient = dynamoDBClient2;
	}

	@Override
	public void initialize(InitializationInput initializationInput) {
		//System.out.println("Shard ID - " + initializationInput.getShardId());
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		//saveContextData(processRecordsInput);
		processDelay(processRecordsInput);
	}

	private void processDelay(ProcessRecordsInput processRecordsInput) {
		List<Record> records = processRecordsInput.getRecords();
		for(Record record: records) {
			//CompletableFuture.runAsync(() -> {
				new BatchAnalysis(dynamoDBClient, processRecordsInput).doReporting();
			//});
            prevRecord = record;   // Change prev status
		}
	}

	void saveContextData(ProcessRecordsInput processRecordsInput) {
		List<Record> records = processRecordsInput.getRecords();
		for(Record record: records) {
			String data = new String(record.getData().array(), StandardCharsets.UTF_8);
			save(data);
            prevRecord = record;   // Change prev status
		}
	}
	
	private void save(String feed) {
		CompletableFuture.runAsync(() -> {
		    WorkerFeed worker = new WorkerFeed(feed);
		    Map<String, Object> json = worker.process();
		    if(null == json || json.isEmpty()) return;  // TODO exception handling
		    Item item = Item.fromMap(json);
		    DynamoDB dynamodb = new DynamoDB(dynamoDBClient);
		    dynamodb.getTable("vrex-context-storage").putItem(item);
		});
	}
	
	public void putItem(String tableName, String id, String val) {
        java.util.Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("prime", new AttributeValue().withS(id));
        item.put("data", new AttributeValue().withS(val));

        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
        dynamoDBClient.putItem(putItemRequest);
    }

	@Override
	public void shutdown(ShutdownInput shutdownInput) {
		ShutdownReason reason = shutdownInput.getShutdownReason();
        switch (reason) {
            case TERMINATE:	// Re-sharding
            case REQUESTED:	// App shutdown
                checkpoint(shutdownInput.getCheckpointer(), prevRecord);
                break;
            case ZOMBIE:	// Processing will be moved to a different record processor
                break;
        }
	}
	
	private void checkpoint(IRecordProcessorCheckpointer checkpointer, Record record) {
        if (record == null)
            return;
        try {
            checkpointer.checkpoint(record);
        } catch (InvalidStateException e) {
            // Table does not exists
            e.printStackTrace();
        } catch (ShutdownException e) {
            // Two processors are processing the same shard
            e.printStackTrace();
        }
    }
}
