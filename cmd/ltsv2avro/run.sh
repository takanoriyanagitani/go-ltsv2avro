#!/bin/sh

ltsvcols1(){
	echo time:2025-01-22T17:02:14.012345+09:00
	echo service_name:user service
	echo http_method:GET
	echo http_status:200
	echo host_name:user-svc-main
	echo user_name:postgres
	echo pid:299792458
	echo msg:got
	echo level:INFO
	echo tag:read
	echo tag:user
	echo tag:http-client
}

ltsvcols2(){
	echo time:2025-01-22T16:02:14.012345+09:00
	echo service_name:user svc
	echo http_method:POST
	echo http_status:404
	echo host_name:user-svc-main
	echo user_name:postgres
	echo pid:101325
	echo msg:no posts available
	echo level:WARN
	echo tag:write
	echo tag:read
	echo tag:user
	echo tag:http-client
}

ltsvrows(){
	ltsvcols1 | tr '\n' '\t' | sed 's/	$/\n/'
	ltsvcols2 | tr '\n' '\t' | sed 's/	$/\n/'
}

export ENV_LABEL_TIMESTAMP=time
export ENV_LABEL_SEVERITY=level
export ENV_LABEL_BODY=msg
export ENV_LABEL_ATTR=attributes

export ENV_LABEL_TAG=tag

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

ltsvrows |
	./ltsv2avro |
	rq -aJ |
	dasel --read=json --write=yaml |
	bat --language=yaml
