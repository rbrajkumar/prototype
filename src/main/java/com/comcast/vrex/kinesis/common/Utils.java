package com.comcast.vrex.kinesis.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {
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
    
    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(5);
    }
    
    private static final Random RANDOM = new Random();
}
