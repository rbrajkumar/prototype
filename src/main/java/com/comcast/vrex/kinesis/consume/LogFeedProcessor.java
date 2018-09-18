package com.comcast.vrex.kinesis.consume;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

public class LogFeedProcessor implements IRecordProcessor {

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	@Override
	public void initialize(InitializationInput initializationInput) {
		System.out.println("Shard ID - " + initializationInput.getShardId());
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		List<Record> records = processRecordsInput.getRecords();
		for(Record r: records) System.out.println("Record - " + new String(r.getData().array(), StandardCharsets.UTF_8));
	}

	@Override
	public void shutdown(ShutdownInput shutdownInput) {
		
	}

}
