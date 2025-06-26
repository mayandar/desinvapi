#!/bin/bash
# chkconfig: 2345 20 80
# description: DesInventar - SFM API  - 2021-07-10
# Source function library.
#. /etc/init.d/functions
# Some things that run always
touch /var/lock/disapi
# Use this port
PORT=8081

start() {
	echo “Starting DI-SFM API: disapi”
	/usr/bin/java -Dserver.port=$PORT -jar ./disapi-1.0.0.jar
}

stop() {
	echo “Stopping DI-SFM API: disapi”
	kill `pgrep -f disapi-1.0.0.jar`
}

case "$1" in 
    start)
       start
       ;;
    stop)
       stop
       ;;
    restart)
       stop
       start
       ;;
    status)
       # code to check status of app comes here 
       ;;
    *)
       echo "Usage: $0 {start|stop|status|restart}"
esac

exit 0 
