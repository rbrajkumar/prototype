package com.comcast.vrex.kinesis.consume.analysis;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BatchAnalysis2 {

	private final ProcessRecordsInput processRecordsInput;
	private final AmazonDynamoDB dynamoDBClient;
	
	public BatchAnalysis2(AmazonDynamoDB dynamoDBClient, ProcessRecordsInput processRecordsInput) {
		this.processRecordsInput = processRecordsInput;
		this.dynamoDBClient = dynamoDBClient;
	}
	
	public void doReporting() {
		List<Record> records = processRecordsInput.getRecords();
		Map<String, Object> report = new HashMap<String, Object>();
		long now_milli = System.currentTimeMillis();
		
		int limit = 0;
		
		for(Record record: records) {
			String feed = new String(record.getData().array(), StandardCharsets.UTF_8);
			String classification = classifyFeed(feed);
			
			if(null != classification) {  // discard all others
				Object object = report.get(classification);
				EventTypes events = null;
				if(null == object) {
					events = new EventTypes();
					report.put(classification, events);
				} else events = (EventTypes) object;
				
				//EventTypes events = (null != object) ? (EventTypes) object : new EventTypes();
				
				long log_milli = getTimestamp(feed.substring(0, 19));
				long arr_milli = record.getApproximateArrivalTimestamp().getTime();
				
				Date a = record.getApproximateArrivalTimestamp();
				System.out.println(a.getTime() + "arrival" + a.toString());
				System.out.println(now_milli   + "log    " + new Timestamp(now_milli).toString());
				
				if(null == report.get("earliest") || (null != report.get("earliest") && ((Long)report.get("earliest")).longValue() < log_milli)) report.put("earliest", log_milli);
				if(null == report.get("latest") || (null != report.get("latest") && ((Long)report.get("latest")).longValue() > log_milli)) report.put("latest", log_milli);
				
				long proc_delay = log_milli - now_milli;
				
				System.out.println(proc_delay/1000);
				safeIncrement(events.getProc_delay(), proc_delay); // todo
				
				if(!Objects.isNull(arr_milli)) {
					long arr_delay = arr_milli - log_milli;
					safeIncrement(events.getArrival_delay(), arr_delay);
					long proc_delta = proc_delay - arr_delay;
					safeIncrement(events.getProc_delta(), proc_delta);
				}
				proc_delay = now_milli - log_milli;
				safeIncrement(events.getProc_delay(), proc_delay);
				consoleOut(report);
				
				limit++;
				if(limit > 5) System.exit(0);
			}
		}
		
		//if(null == report || report.isEmpty()) return;  // TODO exception handling
		
		long posted = System.currentTimeMillis();
		report.put("posted", new Long(posted));
		report.put("expDateTime", new Long(posted + 691200));
		report.put("requestId", UUID.randomUUID().toString());
		
		// Save
		consoleOut(report);
		//save(report);
	    System.out.println("Done");
	}
	
	void consoleOut(Map<String, Object> report) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			String out = mapper.writeValueAsString(report);
			System.out.println(out);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	private void save(Map<String, Object> report) {
		Item item = Item.fromMap(report);
	    DynamoDB dynamodb = new DynamoDB(dynamoDBClient);
	    dynamodb.getTable("xre_delay_data").putItem(item);
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
	    	//System.out.println(timeString);
	        date = df.parse(timeString);
	    } catch (ParseException e) {
	        throw new RuntimeException("Failed to parse date: ", e);
	    }
		return date.getTime();
	}
	
	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	
	public BatchAnalysis2() {
		processRecordsInput = null;
		this.dynamoDBClient = null;
	}

	private String getFormatedKey(long key) {
		double key_sec = key/100;
		BigDecimal a = new BigDecimal(String.valueOf(key_sec));
		BigDecimal b = a.setScale(2, RoundingMode.DOWN);
		return b.toString();
	}
	
	public double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	
	public static void main(String[] args) {
		new BatchAnalysis2().test();
	}

	private void test() {
		long rawKey = 444444456;
		double no = 44.4546;
		System.out.println(round(no, 1));
	}
	
}