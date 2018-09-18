package com.comcast.vrex.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {
    private static final Random RANDOM = new Random();
    
    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(5);
    }
    
    /**
     * Generates a blob containing a UTF-8 string. The string begins with the
     * sequence number in decimal notation, followed by a space, followed by
     * padding.
     * 
     * @param sequenceNumber
     *            The sequence number to place at the beginning of the record
     *            data.
     * @param totalLen
     *            Total length of the data. After the sequence number, padding
     *            is added until this length is reached.
     * @return ByteBuffer containing the blob
     */
    public static ByteBuffer generateData(long sequenceNumber, int totalLen) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(sequenceNumber));
        sb.append(" ");
        while (sb.length() < totalLen) {
            sb.append("a");
        }
        try {
            return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String generateData(long sequenceNumber) {
    	String data = null;
    	try {
    		data = readLinesFromFile("xre_events.log").get((int) sequenceNumber);
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return data;
    }
    
    public static List<String> readLinesFromFile(String fileName) throws Exception{
    	File file = new File(fileName);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		List<String> list = new ArrayList<String>();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			list.add(line);
		}
		fileReader.close();
		return list;
    }
}