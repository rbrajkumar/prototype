package com.company.dept.prototype;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.company.dept.prototype.model.HoldParam;
import com.company.dept.prototype.service.TicketService;

@RestController
@RequestMapping("/ticket")
public class TicketController {

	@GetMapping("/seats-available")
	public int getAvailability() {
		return ticketing.numSeatsAvailable();
	}
	
	@PostMapping(value = "/hold-seats")
	@ResponseStatus(HttpStatus.CREATED)
	public void hold(@RequestBody HoldParam param) {
		
	}	
	
	@Autowired
	private TicketService ticketing;
	
}
