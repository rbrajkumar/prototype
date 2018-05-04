package com.company.dept.prototype.model;

import com.company.dept.prototype.util.CommonUtil;

public class SeatHold {
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public SeatHold() {   // Used by mock/unit testing
		super();
	}

	public SeatHold(String key) {
		super();
		String[] attibutes = key.split(_SEPARATOR);
		//if(null == attibutes || attibutes.length != 4) throw new RuntimeException("Invalid hold id");
		
		this.id = Integer.parseInt(attibutes[0]);
		this.email = attibutes[1];
		this.count = Integer.parseInt(attibutes[2]);
	}
	
	public SeatHold(int count, String email) {
		super();
		this.count = count;
		this.email = email;
		this.id = CommonUtil.generateUniqueId();
	}
	
	public String getKey() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(getId()).append(":");
		buffer.append(getEmail()).append(":");
		buffer.append(getCount());
		return buffer.toString();
	}

	private int id;
	private int count;
	private String email;
	
	private static final String _SEPARATOR = ":";   // if more, then goes to separate constants file
}
