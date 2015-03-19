/*
Package reprow provides reverse proxy implementation for Message Queue workers.
Message queue architecture is becoming even more common with  soa/microservice architecture.

But current implementations of Job workers have following problems
 * Implementations/Protocol/framework has programming language x backend(Redis/SQS/Q4M etc)  variations
 * It enforces application to manage pull driven workers(i.e Workers) and push based workers(i.e API) at the same time

Ideally, Pull driven job managing should not be done in application layers.
It should rather be done by middlewares and the middleware that processes job should speak to application with standard protocols(such as HTTP)

Using reprow, you can achieve following architecture

	JobBackend                          reprow                         Application
	               <- pulls a job
	                                              -> HTTP Proxy
	                                              <- Returns response
	               <- abort or ends Job

With the above architecture, application developers can handle both job messages and usual client requests with HTTP API.

To use it you should prepare config files as in https://github.com/maedama/reprow/tree/master/sample .

And execute with

	reprow -c sample/q4m,yaml

*/
package reprow
