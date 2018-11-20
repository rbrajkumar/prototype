package com.comcast.vrex.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.comcast.vrex.consumer.KinesisConsumer;

@RestController
@RequestMapping("/xre/context-feed")
public class XREStreamController {
	
	@Autowired
	private KinesisConsumer kinesis;
	
	@GetMapping("/hc")
	public String getHealthInfo() {
		return "ok";
	}
	
	@GetMapping("/read")
    public String start(){
		kinesis.readStream("xre_log_to_vrex_v2", "us-west-2", "xre-log-analyser-west-test1");
		return null;
    }
	
}
