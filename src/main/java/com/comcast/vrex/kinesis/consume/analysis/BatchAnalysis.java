package com.comcast.vrex.kinesis.consume.analysis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BatchAnalysis {

	private final ProcessRecordsInput processRecordsInput;
	private final AmazonDynamoDB dynamoDBClient;
	
	public BatchAnalysis(AmazonDynamoDB dynamoDBClient, ProcessRecordsInput processRecordsInput) {
		this.processRecordsInput = processRecordsInput;
		this.dynamoDBClient = dynamoDBClient;
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public void doReporting() {
		List<Record> records = processRecordsInput.getRecords();
		Map<String, Object> report = new HashMap<String, Object>();
		long now_milli = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
		
		long earliest = 0, latest = 0;
		for(Record record: records) {
			String feed = new String(record.getData().array(), StandardCharsets.UTF_8);
			String classification = classifyFeed(feed);
			
			if(null != classification) {  // discard all others
				Object object = report.get(classification);
				
				EventTypes events = null;
				if(null == object) {
					events = new EventTypes();
					report.put(classification, events);
				} else {
					events = (EventTypes) object;
				}
				
				long log_milli = getTimestamp(feed.substring(0, 23));
				long arr_milli = record.getApproximateArrivalTimestamp().getTime();
				
				if(earliest < log_milli) earliest = log_milli;
				if(latest == 0 || latest > log_milli) latest = log_milli;
				
				long proc_delay = now_milli - log_milli;
				safeIncrement(events.getProc_delay(), proc_delay);
				
				if(!Objects.isNull(arr_milli)) {  // just translation, in java it is always true :)
					long arr_delay = arr_milli - log_milli;
					safeIncrement(events.getArrival_delay(), arr_delay);
					long proc_delta = proc_delay - arr_delay;
					safeIncrement(events.getProc_delta(), proc_delta);
				}
				proc_delay = now_milli - log_milli;
				safeIncrement(events.getProc_delay(), proc_delay);
			}
		}
		
		long posted = System.currentTimeMillis();
		report.put("posted", posted);
		report.put("expDateTime", (posted + 691200));
		report.put("requestId", UUID.randomUUID().toString());
		report.put("timeSpentInCache", processRecordsInput.getTimeSpentInCache().getNano());
		
		// Save
		String json = transform(report);
		if(null != json) save(json);
	}
	
	String transform(Map<String, Object> report) {
		ObjectMapper mapper = new ObjectMapper();
		String out = null;
		try {
			out = mapper.writeValueAsString(report);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return out;
	}

	private void save(String report) {
		System.out.println(report);
		Item item = Item.fromJSON(report);
	    DynamoDB dynamodb = new DynamoDB(dynamoDBClient);
	    dynamodb.getTable("xre_delay_data_e1").putItem(item);
	}
	
	private String getFormatedKey(long key) {
		double key_sec = key/1000;
		BigDecimal a = new BigDecimal(String.valueOf(key_sec));
		a = a.setScale(1, RoundingMode.HALF_UP);
		return a.toString();
	}
	
	private void safeIncrement(Map<String, Integer> map, long rawKey) {
		String key = getFormatedKey(rawKey);
		Integer past = map.get(key);
		if(null == past) map.put(key, 1);
		else map.put(key, past.intValue()+1);
	}
	
	class EventTypes {
		private Map<String, Integer> arrival_delay = new HashMap<String, Integer>();
		private Map<String, Integer> proc_delay = new HashMap<String, Integer>();
		private Map<String, Integer> proc_delta = new HashMap<String, Integer>();
		public Map<String, Integer> getArrival_delay() {
			return arrival_delay;
		}
		public void setArrival_delay(Map<String, Integer> arrival_delay) {
			this.arrival_delay = arrival_delay;
		}
		public Map<String, Integer> getProc_delay() {
			return proc_delay;
		}
		public void setProc_delay(Map<String, Integer> proc_delay) {
			this.proc_delay = proc_delay;
		}
		public Map<String, Integer> getProc_delta() {
			return proc_delta;
		}
		public void setProc_delta(Map<String, Integer> proc_delta) {
			this.proc_delta = proc_delta;
		}
		@Override
		public String toString() {
			return "EventTypes [arrival_delay=" + arrival_delay + ", proc_delay=" + proc_delay + ", proc_delta="
					+ proc_delta + "]";
		}
	}
	
	private String classifyFeed(String feed) {
		String type = null;
		if(feed.indexOf("MEDIATUNE_500") !=-1 && feed.indexOf("tflType='Media Tune Metrics'") != -1) {
			// ['MEDIATUNE_500', 'tflType=\'Media Tune Metrics\'']
			type = "MEDIATUNE_500";
		} else if(feed.indexOf("STA_054") !=-1 || feed.indexOf("tflType='Last State'") != -1) {
			if(feed.indexOf("ownerClass=ApplicationManager") !=-1 || feed.indexOf("ownerClass=AppUiContainerManager") != -1) {
				if(feed.indexOf("state=LOADING") !=-1) {
					//  ['STA_054', 'tflType=\'Last State\'', 'ownerClass=ApplicationManager', 'state=LOADING']
					type = "STA_054:Application:LOADING";
				} else if(feed.indexOf("state=TERMINATED") !=-1) {
					// ['STA_054', 'tflType=\'Last State\'', 'ownerClass=ApplicationManager', 'state=TERMINATED']
					type = "STA_054:Application:TERMINATED";
				}
			} else if(feed.indexOf("ownerClass=DeepLinkManager") !=-1 && feed.indexOf("deeplink='xre:///guide/x2/") !=-1) {
				// ['STA_054', 'tflType=\'Last State\'', 'ownerClass=DeepLinkManager', 'deeplink=\'xre:///guide/x2/']
				type = "STA_054:entity_page";
			}
		} else if(feed.indexOf("SCE_055") !=-1 && feed.indexOf("tflType='Last Scene'") !=-1 && feed.indexOf("ownerClass=SceneController") !=-1) {
			// ['SCE_055', 'tflType=\'Last Scene\'', 'ownerClass=SceneController']
			type = "SCE_055";
		}
		
		return type;
	}
		
	private long getTimestamp(String timeString) {
		Date date;
	    try {
	    	date = df.parse(timeString);
	        //System.out.println(timeString + " - " + date.getTime());
	    } catch (ParseException e) {
	        throw new RuntimeException("Failed to parse date: ", e);
	    }
		return date.getTime();
	}
	
	/*private  Date localToGmt( Date date ){
	    TimeZone tz = TimeZone.getDefault();
	    Date ret = new Date( date.getTime() - tz.getRawOffset() );

	    // if we are now in DST, back off by the delta.  Note that we are checking the GMT date, this is the KEY.
	    if ( tz.inDaylightTime( ret )){
	        Date dstDate = new Date( ret.getTime() - tz.getDSTSavings() );

	        // check to make sure we have not crossed back into standard time
	        // this happens when we are on the cusp of DST (7pm the day before the change for PDT)
	        if ( tz.inDaylightTime( dstDate )){
	            ret = dstDate;
	        }
	     }
	     return ret;
	}*/
	
	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
}