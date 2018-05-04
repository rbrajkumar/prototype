package com.company.dept.prototype.util;

import java.util.concurrent.TimeUnit;

import com.company.dept.prototype.model.SeatHold;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CacheUtil {
	
	private static LoadingCache<String, String> cache;
	
	static {
		
		cache = CacheBuilder.newBuilder()						// May be higher, but less than auditorium total seats. but preferred way is make DB call
			       .maximumSize(100)   							// based on auditorium for max seats for more real time.
			       .expireAfterWrite(5000, TimeUnit.MILLISECONDS)
			       .build(
			           new CacheLoader<String, String>() {
							@Override
							public String load(String key) throws Exception {
								return key.toUpperCase();
							}
			           }
			       );
    }
	
	public static LoadingCache<String, String> getCache() {
		return cache;
    }
}
