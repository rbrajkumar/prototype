# prototype

Spring based legacy style Implementation of Ticketing system.

Assumption:
- Deployed in Single server: Since no need of DB/centralized cache for prototype,
- Syntax/Symantic level Validation and exception handling is not required due to scope,

Instructions:
- Download and Install STS with mavan,
- Due running test cases, expiry time in milliseconds. 
	Please change it in below places 
		1. TicketServiceImpl,
		2. TicketControllerTests