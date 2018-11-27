package com.comcast.vrex.consumer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class StreamEventProcessor {
	
	private DynamoDbAsyncClient dbClient;
	private String tableName;
	
	public StreamEventProcessor(DynamoDbAsyncClient dbClient, String tableName) {
		super();
		this.dbClient = dbClient;
		this.tableName = tableName;
	}

	public void processEvent(SubscribeToShardEvent event) {
		for(Record record : event.records()) {
			processRecord(record.data().asUtf8String());
		}
	}
	
	void processRecord(String record) {
		Map<String, Object> map = getResults(record);
		Map<String, AttributeValue> converted = convert(map);
		dbClient.putItem(PutItemRequest.builder().tableName(tableName).item(converted).build());
	}
	
	Map<String, AttributeValue> convert(Map<String, Object> classic) {
		Map<String, AttributeValue> out = new HashMap<String, AttributeValue>();
		for(Map.Entry<String, Object> entry : classic.entrySet()) {
			if(entry.getKey().equals("context")) {
				continue;
			}
			out.put(entry.getKey(), AttributeValue.builder().s(entry.getValue().toString()).build());
		}
		
		Map<String, AttributeValue> cnxt = new HashMap<String, AttributeValue>();
		Map<String, Object> contxtMap = (Map<String, Object>) classic.get("context");
		for(Map.Entry<String, Object> entry : contxtMap.entrySet()) {
			if(entry.getValue() instanceof String) {
				cnxt.put(entry.getKey(), AttributeValue.builder().s(entry.getValue().toString()).build());
			} else {
				cnxt.put(entry.getKey(), AttributeValue.builder().n(entry.getValue().toString()).build());
			}
		}
		out.put("context", AttributeValue.builder().m(cnxt).build());
		return out;
	}
	
	private Map<String, Object> getResults(String logLine) {
		Map<String, Object> map = null;
		if(logLine.indexOf("MEDIATUNE_500") !=-1 && logLine.indexOf("tflType='Media Tune Metrics'") != -1) {
			// ['MEDIATUNE_500', 'tflType=\'Media Tune Metrics\'']
			map = parseMEDIATUNE500(logLine);
		} else if(logLine.indexOf("STA_054") !=-1 || logLine.indexOf("tflType='Last State'") != -1) {
			if(logLine.indexOf("ownerClass=ApplicationManager") !=-1) {
				if(logLine.indexOf("state=LOADING") !=-1) {
					//  ['STA_054', 'tflType=\'Last State\'', 'ownerClass=ApplicationManager', 'state=LOADING']
					map = parseSTA054ApplicationLoading(logLine);
				} else if(logLine.indexOf("state=TERMINATED") !=-1) {
					// ['STA_054', 'tflType=\'Last State\'', 'ownerClass=ApplicationManager', 'state=TERMINATED']
					map = parseSTA054ApplicationTerminated(logLine);
				}
			} else if(logLine.indexOf("ownerClass=DeepLinkManager") !=-1 && logLine.indexOf("deeplink='xre:///guide/x2/") !=-1) {
				// ['STA_054', 'tflType=\'Last State\'', 'ownerClass=DeepLinkManager', 'deeplink=\'xre:///guide/x2/']
				map = parseSTA054EntityPage(logLine);
			}
		} else if(logLine.indexOf("SCE_055") !=-1 && logLine.indexOf("tflType='Last Scene'") !=-1 && logLine.indexOf("ownerClass=SceneController") !=-1) {
			// ['SCE_055', 'tflType=\'Last Scene\'', 'ownerClass=SceneController']
			map = parseSCE055EntityPageState(logLine);
		}
		
		return map;
	}
	
	private Map<String, Object> parseSCE055EntityPageState(String logLine) {
		Map<String, Object> map = parseAttributes("xre:event:SCE_055:entity_page_sate", "SCE_055", logLine);
		Map<String, Object> context = (Map<String, Object>) map.get("context");
			
		String previousScene = extractValue("previousScene", logLine);    //TODO check for extraction
		if(!previousScene.isEmpty()) context.put("previousScene", previousScene);
		
		String activeScene = extractValue("activeScene", logLine);
		if(!activeScene.isEmpty()) context.put("activeScene", activeScene);
		
		return map;
	}

	private Map<String, Object> parseSTA054EntityPage(String logLine) {
		Map<String, Object> map = parseAttributes("xre:event:STA_054:entity_page", "STA_054:entity_page", logLine);
		String deeplink = extractValue("deeplink", logLine);
		if(!deeplink.isEmpty()) {
			Map<String, Object> context = (Map<String, Object>) map.get("context");
			
			String entityId = extractValue("entityId", deeplink);    //TODO check for extraction
			if(!entityId.isEmpty()) context.put("entityId", entityId);
			
			String programId = extractValue("entityType", deeplink);
			if(!programId.isEmpty()) context.put("entityType", programId);
		}
		return map;
	}

	private Map<String, Object> parseSTA054ApplicationTerminated(String logLine) {
		String category = "xre:event:STA_054:Application:TERMINATED";
		Map<String, Object> map = parseAttributes(category, "STA_054:Application:TERMINATED", logLine);
				
		String appId = extractValue("appId", logLine);
		if(!appId.isEmpty()) {
			Map<String, Object> context = (Map<String, Object>)map.get("context");
			context.put("appId", appId);
			map.put("category", category + ":" + appId);
		}
		return map;
	}

	private Map<String, Object> parseSTA054ApplicationLoading(String logLine) {
		String category = "xre:event:STA_054:Application:LOADING";
		Map<String, Object> map = parseAttributes(category, "STA_054:Application:LOADING", logLine);
				
		String appId = extractValue("appId", logLine);
		if(!appId.isEmpty()) {
			Map<String, Object> context = (Map<String, Object>)map.get("context");
			context.put("appId", appId);
			map.put("category", category + ":" + appId);
		}
		return map;
	}

	private Map<String, Object> parseMEDIATUNE500(String logLine) {
		Map<String, Object> map = parseAttributes("xre:event:MEDIATUNE_500", "MEDIATUNE_500", logLine);	
		Map<String, Object> context = (Map<String, Object>)map.get("context");
		
		String playBackMode = extractValue("playBackMode", logLine);
		if(!playBackMode.isEmpty()) context.put("playBackMode", playBackMode);
		
		String stationId = extractValue("stationId", logLine);
		if(!stationId.isEmpty()) context.put("stationId", stationId);
		
		String programId = extractValue("programId", logLine);
		if(!programId.isEmpty()) context.put("programId", programId);
		
		String mediaGUID = extractValue("mediaGUID", logLine);
		if(!mediaGUID.isEmpty()) context.put("mediaGUID", mediaGUID);
		
		String listingId = extractValue("listingId", logLine);
		if(!listingId.isEmpty()) context.put("listingId", listingId);
		
		return map;
	}

	private Map<String, Object> parseAttributes(String category, String eventType, String line) {
		String created = line.substring(0, 19);
		long ts = getTimestamp(created);
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("deviceId", extractValue("deviceId", line));
		
		result.put("category", category);
		result.put("created", created);
		
		String receiverId = extractValue("receiverId", line);
		if(!receiverId.isEmpty()) result.put("receiverId", receiverId);
		
		String accountId = extractValue("accountId", line);
		if(!accountId.isEmpty()) result.put("accountId", accountId);
		
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("eventType", eventType);
		context.put("ts", new Long(ts));
		result.put("context", context);
		
		return result;
	}

	private long getTimestamp(String timeString) {
		Date date;
	    try {
	        date = df.parse(timeString);
	    } catch (ParseException e) {
	        throw new RuntimeException("Failed to parse date: ", e);
	    }
		return date.getTime();
	}

	private String extractValue(String key, String src) {
		if(src.contains(key)) {
			int startIndex = src.indexOf(key);
			startIndex = src.indexOf("=", startIndex);
			char c = src.charAt(startIndex+1);
			String Spliter = " ";
			boolean cut = false;
			if(String.valueOf(c).equals("'") || String.valueOf(c).equals("\"")) {
				Spliter = String.valueOf(c);
				startIndex++;
				cut = true;
			}
			
			String out = src.substring(startIndex+1, src.indexOf(Spliter, startIndex+2));
			if (cut)return out;
			else if(out.contains(",")) out = out.substring(0, out.indexOf(","));
			return out;
		}
		return "";
	}
	
	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Logger logger = LoggerFactory.getLogger(StreamEventProcessor.class);
}
