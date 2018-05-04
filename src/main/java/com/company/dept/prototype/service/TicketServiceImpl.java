package com.company.dept.prototype.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.company.dept.prototype.model.SeatHold;
import com.company.dept.prototype.util.CacheUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Service
public class TicketServiceImpl extends BaseService implements TicketService {

	static {
		cache = CacheBuilder.newBuilder()
			       .maximumSize(100)                                    // ideally both get it from DB instead of any property file
			       .expireAfterWrite(1000, TimeUnit.MILLISECONDS)       // change expire period if needed
			       .build(
			           new CacheLoader<String, String>() {
							@Override
							public String load(String key) throws Exception {
								return key.toUpperCase();
							}
			           }
			       );
    }
	
	@Override
	public int numSeatsAvailable() {
		cache.cleanUp();
		int size = (int)cache.size();
		return totalSeats - size;
	}

	@Override
	public SeatHold findAndHoldSeats(int numSeats, String customerEmail) {
		SeatHold hold = new SeatHold(numSeats, customerEmail);
		String key = hold.getKey(), subKey = "";
		List<String> keys = new ArrayList<String>();
		
		LoadingCache<String, String> cache = getCache();
		for(int i = 0; i < numSeats; i++) {
			subKey = key + ":" + i;		// for identify the total left
			cache.getUnchecked(subKey);
			keys.add(subKey);
		}
		statusMap.put(new Integer(hold.getId()), keys); 
		return hold;
	}

	@Override
	public String reserveSeats(int seatHoldId, String customerEmail) {
		List<String> keys = statusMap.get(new Integer(seatHoldId));
		if(null == keys || keys.isEmpty()) {
			//Expired, throw exception like 'hold not found' using existing application framework
			throw new RuntimeException("hold/reservation id not found");
		} else {
			String last = "";
			for(String key: keys) {
				if(null == getCache().getIfPresent(key)) {
					throw new RuntimeException("Hold/reservation expired");
				} else {
					// now ticket will be sold, then
					getCache().invalidate(key);
				}
				last = key;
			}
			// clear references
			SeatHold hold = new SeatHold(last);
			totalSeats = totalSeats - hold.getCount();
			statusMap.remove(new Integer(seatHoldId));
		}
		return (seatHoldId + "CONFIRMED");  // just for demo(for < complex), confirmCode = holdid + "CONFIRMED"
	}
	
	
	@Override
	public void flush() {
		getCache().invalidateAll();
		statusMap.clear();
	}
	
	public static LoadingCache<String, String> getCache() {
		cache.cleanUp();
		return cache;
    }

	// Bellow are usually DB tables in real world scenarios
	private static LoadingCache<String, String> cache;	
	private static Map<Integer, List<String>> statusMap = new ConcurrentHashMap<Integer, List<String>>();
}
