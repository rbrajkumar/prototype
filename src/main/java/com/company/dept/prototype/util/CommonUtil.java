package com.company.dept.prototype.util;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CommonUtil {

	public static final int generateUniqueId() {  // Preferred UUID but as per interface int data type
	    StringBuilder sb = new StringBuilder(5);
	    for (int i = 0; i < 5; i++)
	      sb.append(AB.charAt(rnd.nextInt(AB.length())));
	    return Integer.parseInt(sb.toString());
	}
	
	public static String asJsonString(final Object obj) {
	    try {
	        final ObjectMapper mapper = new ObjectMapper();
	        final String jsonContent = mapper.writeValueAsString(obj);
	        return jsonContent;
	    } catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	}
	
	public static Map<String, Object> getMap(String jsonStr) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = mapper.readValue(jsonStr, new HashMap<String, Object>().getClass());
		return map;
	}
	
	private static final String AB = "01234567899876543210";
	private static SecureRandom rnd = new SecureRandom();
}
