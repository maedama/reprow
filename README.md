# About

reprow provides reverse proxy implementation for Message Queue workers.

Message queue architecture is becoming even more common with  soa/microservice architecture.

But current implementations of Job workers have following problems
 * Implementations/Protocol/framework has programming language x backend(Redis/SQS/Q4M etc)  variations
 * It enforces application to manage pull triggered workers(i.e Workers) and push based workers(i.e API) at the same time.

Ideally, life cycle of job management(Pull or Push) should be done in reverse proxy layer and not in application layer.

Application should focus on executing each tasks and report reverse proxy if it success/failed providing some more informations on failure(i.e retry after).

Reprow provides reverse proxy layer for pull based job workers to allows us to have these architecture as followings

	JobBackend                          reprow                         Application
	               <- pulls a job
	                                              -> HTTP Proxy
	                                              <- Returns response
	               <- abort or ends Job

With the above architecture, application developers can handle both job messages and usual client requests with HTTP API.

# Install

```
    go get github.com/maedama/reprow/cmd/reprow
```

# Queue Backends

Currently it provides folloing backends

* SQS(http://aws.amazon.com/jp/sqs/)
* Q4M(https://github.com/q4m/q4m/)
* Linux Fifo(mainly for development)

# Runners

It provides http proxy runners only.


## Examples
### Running worker sample server

To ilustrate how it works, there is a sample worker server using Perl

```
   plackup sample/worker.psgi
```


### Running with sqs as backend
see https://github.com/maedama/reprow/blob/master/sample/sqs.yaml for configuration

```
    reprow -c sample/sqs.yaml
```

### Running with q4m as backend
```
    reprow -c sample/q4m.yaml
```

### Running with fifo as backend

This is mainly used as development
```
    mkfifo /tmp/queue
    reprow -c sample/q4m.yaml
```



# Development

## Q4M

Install instruction can be seen in official page.
But if you are using centos q4m can easily be installed by https://github.com/kazeburo/mysetup and test executed like
```
MYSQLD=/usr/local/q4m/libexec/mysqld MYSQL_INSTALL_DB=/usr/local/q4m/bin/mysql_install_db go test -v ./...
```

