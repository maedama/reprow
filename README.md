# About

reprow provides reverse proxy implementation for Message Queue workers.

Message queue architecture is becoming even more common with  soa/microservice architecture.

But current implementations of Job workers have following problems
 * Implementations/Protocol/framework has programming language x backend(Redis/SQS/Q4M etc)  variations
 * It enforces application to manage pull triggered workers(i.e Workers) and push triggered workers(i.e API) at the same time.


Unlike most of worker frameworks, reprow work as a proxy server for job message.

Instead of listening to http socket and reverse proxying it to application server like nginx,
it pulls job from backend queue and  proxies it to application server with HTTP.

By doing so it can provide:

* Language/Queue backend(i.e SQS) independent frameworks for job queue workers
* Using standard protocols (HTTP) to work well with soa/microservice architecture

By using reprow, job workers architecture would look very similar to that of HTTP Worker serving client request.
```
	JobBackend                          reprow                         Application
	               <- pulls a job
	                                              -> HTTP Proxy
	                                              <- Returns response
	               <- abort or ends Job
```

# Install

```
    go get github.com/maedama/reprow/cmd/reprow
```



# Queue Backends

Reprow has Multiple queue backend implementations. Currently it provides folloing backends

* SQS(http://aws.amazon.com/jp/sqs/)
* Q4M(https://github.com/q4m/q4m/)
* Linux Fifo(mainly for development)

# Runners

Reprow has runners that is pluggable. Runner communicates with application server inorder to pass job and retrieve job execution responses.
Currently it provides http proxy runners only.


## Examples
### Running sample application server
To illustrate how it works, there is a sample worker server using Perl.
Worker server should receive job with HTTP and if job is successful, it should return with HTTP 200 Status code,
If job fails it should return with HTTP 500 Status code.

Optionally SQS backend supports retry after, which in this case application servre may return Retry-After Header specifying how many seconds, it should take for job to be reprocessed.

```
   plackup sample/worker.psgi
```


### Running with sqs as backend
see https://github.com/maedama/reprow/blob/master/sample/sqs.yaml for configuration

```
    reprow -c sample/sqs.yaml
```

### Running with q4m as backend
see https://github.com/maedama/reprow/blob/master/sample/q4m.yaml for configuration
```
    reprow -c sample/q4m.yaml
```

### Running with fifo as backend
This is mainly used as development.

see https://github.com/maedama/reprow/blob/master/sample/fifo.yaml for configuration
```
    mkfifo /tmp/queue
    reprow -c sample/q4m.yaml
```



# Development

## Q4M

Install instruction can be seen in official page.
But if you are using centos q4m can easily be installed by https://github.com/kazeburo/mysetup and test executed like
```
MYSQLD=/usr/local/q4m/libexec/mysqld \
MYSQL_INSTALL_DB=/usr/local/q4m/bin/mysql_install_db \
go test -v ./...
```

