# prototype

Spring based legacy style Implementation of Ticketing system.

Assumption:
- Deployed in Single server: Since no DB/centralized cache for prototype,
- Syntax/Symantic level Validation and exception handling is not required due to scope,
- No metadata driven approach due to No DB (properties file changes still involves deployment in some cases),

Instructions:
- Download and Install STS with mavan,
- Due running test cases, expiry time in milliseconds. 
	Please change it in below places if you need
		1. TicketServiceImpl,
		2. TicketControllerTests
		
Drive Factor:
   - No Complex and minimalest design and dependencies,
   - Since it's prototype and it's scope, data structure will be on in memory not with file/hard disk
due to concurrency.

Recommendation:
   - For software quality attributes (like portability, scalability, performance etc) it can be designed
 as follows in AWS Cloud
      API Gateway -> Lambda -> dynamoDB (with DAX), In this flow dynamoDB have immutable insertion of duplicate
 record and optimistic locking feature helps lesser coding and better performance wise. or recommending
 serverless microservices based architecture in Azure Cloud with similar native services.