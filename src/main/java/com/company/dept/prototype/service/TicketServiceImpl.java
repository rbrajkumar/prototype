package com.company.dept.prototype.service;

import org.springframework.stereotype.Service;

import com.company.dept.prototype.model.SeatHold;

@Service
public class TicketServiceImpl extends BaseService implements TicketService {

	@Override
	public int numSeatsAvailable() {
		return totalSeats;
	}

	@Override
	public SeatHold findAndHoldSeats(int numSeats, String customerEmail) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String reserveSeats(int seatHoldId, String customerEmail) {
		// TODO Auto-generated method stub
		return null;
	}

}
