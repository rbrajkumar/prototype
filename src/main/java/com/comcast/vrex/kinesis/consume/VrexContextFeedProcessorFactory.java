package com.comcast.vrex.kinesis.consume;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class VrexContextFeedProcessorFactory implements IRecordProcessorFactory {

	@Override
	public IRecordProcessor createProcessor() {
		return new LogFeedProcessor();
	}

}
