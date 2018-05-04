package com.company.dept.prototype;

import static com.company.dept.prototype.util.CommonUtil.asJsonString;
import static com.company.dept.prototype.util.CommonUtil.getMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.company.dept.prototype.model.HoldParam;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class TicketControllerTests {

	@Autowired
    private MockMvc mockMvc;
	
	@Test
	public void totalSeatsShouldNotNULL() throws Exception {
						
		this.mockMvc.perform(get("/ticket/seats-available")).andDo(print()).andExpect(status().is2xxSuccessful())
		.andExpect(content().string(containsString("50")));
		
		// flush all previous test cases
		this.mockMvc.perform(get("/ticket/flush")).andDo(print()).andExpect(status().is2xxSuccessful());
		
	}
	
	@Test
	public void holdSeatsTotalShouldNotNULL() throws Exception {
				
		// create mock
		HoldParam param = new HoldParam();
		param.setEmail("email@email.com");
		param.setNo(2);
		
		// calling rest service
		this.mockMvc.perform(post("/ticket/hold-seats")
			  .content(asJsonString(param))
      		  .contentType(MediaType.APPLICATION_JSON)
      		  .accept(MediaType.APPLICATION_JSON)).andDo(print()).andExpect(status().is2xxSuccessful())
			  .andExpect(content().string(containsString("2")));
		
		this.mockMvc.perform(get("/ticket/seats-available")).andDo(print()).andExpect(status().is2xxSuccessful())
		.andExpect(content().string(containsString("48")));
		
		// flush all previous test cases
		this.mockMvc.perform(get("/ticket/flush")).andDo(print()).andExpect(status().is2xxSuccessful());
				
	}
	
	@Test
	public void holdSeatsCallTotalShouldSame() throws Exception {
		// flush all previous test cases
		this.mockMvc.perform(get("/ticket/flush")).andDo(print()).andExpect(status().is2xxSuccessful());
		
		// create mock
		HoldParam param = new HoldParam();
		param.setEmail("email@email.com");
		param.setNo(2);
		
		// calling rest service
		this.mockMvc.perform(post("/ticket/hold-seats")
			  .content(asJsonString(param))
      		  .contentType(MediaType.APPLICATION_JSON)
      		  .accept(MediaType.APPLICATION_JSON)).andDo(print()).andExpect(status().is2xxSuccessful())
			  .andExpect(content().string(containsString("2")));
		
		Thread.sleep(1500);
		this.mockMvc.perform(get("/ticket/seats-available")).andDo(print()).andExpect(status().is2xxSuccessful())
		.andExpect(content().string(containsString("50")));
		
		// flush all previous test cases
		this.mockMvc.perform(get("/ticket/flush")).andDo(print()).andExpect(status().is2xxSuccessful());
				
	}
	
	// I will add more test cases in the evening
	
}
