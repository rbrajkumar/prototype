package com.company.dept.prototype.service;

import org.springframework.beans.factory.annotation.Value;

public abstract class BaseService {

	@Value("${prototype.app.ticket.capacity.total}")
	protected int totalSeats;
	
}
