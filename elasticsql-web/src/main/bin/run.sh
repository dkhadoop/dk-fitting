#!/bin/sh
JAVA_HOME=$JAVA_HOME
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
APP_HOME=`pwd`

APP_MAINCLASS=com.dksou.DkEssqlApplication

#RUNNING_USER=root
CONF_DIR=$APP_HOME/conf
LOGS_FILE=`sed '/log4j.file/!d;s/.*=//' conf/log4j.properties | tr -d '\r'`
LOGS_DIR=""
if [ -n "$LOGS_FILE" ]; then
    LOGS_DIR=`dirname $LOGS_FILE`
else
    LOGS_DIR=$APP_HOME/logs
fi
if [ ! -d $LOGS_DIR ]; then
    mkdir $LOGS_DIR
fi
STDOUT_FILE=$LOGS_DIR/dksearch.log

LIB_DIR=$APP_HOME/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`

JAVA_OPTS=" -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true "
JAVA_DEBUG_OPTS=""
if [ "$1" = "debug" ]; then
    JAVA_DEBUG_OPTS=" -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n "
fi
JAVA_JMX_OPTS=""
if [ "$1" = "jmx" ]; then
    JAVA_JMX_OPTS=" -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false "
fi
JAVA_MEM_OPTS=""
BITS=`java -version 2>&1 | grep -i 64-bit`
if [ -n "$BITS" ]; then
    JAVA_MEM_OPTS=" -server -Xmx2g -Xms2g -Xmn256m -XX:PermSize=128m -Xss256k -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:LargePageSizeInBytes=128m -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=70 "
else
    JAVA_MEM_OPTS=" -server -Xms1g -Xmx1g -XX:PermSize=128m -XX:SurvivorRatio=2 -XX:+UseParallelGC "
fi

LIB_DIR=$APP_HOME/lib
CLASSPATH=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ":"`
psid=0
checkpid() {
   javaps=`$JAVA_HOME/bin/jps -l | grep $APP_MAINCLASS`

   if [ -n "$javaps" ]; then
      psid=`echo $javaps | awk '{print $1}'`
   else
      psid=0
   fi
}

start() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo "================================"
      echo "warn: $APP_MAINCLASS already started! (pid=$psid)"
      echo "================================"
   else
      echo -n "Starting $APP_MAINCLASS ..."
      nohup $JAVA_HOME/bin/java $JAVA_OPTS $JAVA_MEM_OPTS $JAVA_DEBUG_OPTS $JAVA_JMX_OPTS -classpath $CONF_DIR:$CLASSPATH $APP_MAINCLASS >> $STDOUT_FILE 2>&1 &
#      JAVA_CMD="nohup $JAVA_HOME/bin/java $JAVA_OPTS $JAVA_MEM_OPTS $JAVA_DEBUG_OPTS $JAVA_JMX_OPTS -classpath $CONF_DIR:$CLASSPATH $APP_MAINCLASS >>$STDOUT_FILE 2>&1 &"
#      su - $RUNNING_USER -c "$JAVA_CMD"
      checkpid
      if [ $psid -ne 0 ]; then
         echo "OK"
         echo "pid=$psid"
         echo "STDOUT: $STDOUT_FILE"
      else
         echo "Failed"
      fi
   fi
}


stop() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo -n "Stopping the  $APP_MAINCLASS ... "
      kill -9 $psid
#      su - $RUNNING_USER -c "kill -9 $psid"
      if [ $? -eq 0 ]; then
         echo "OK"
         echo "pid=$psid"
      else
         echo "Failed"
      fi

      checkpid
      if [ $psid -ne 0 ]; then
         stop
      fi
   else
      echo "================================"
      echo "warn: $APP_MAINCLASS is not running!"
      echo "================================"
   fi
}


status() {
   checkpid

   if [ $psid -ne 0 ];  then
      echo "$APP_MAINCLASS is running! (pid=$psid)"
   else
      echo "$APP_MAINCLASS is not running"
   fi
}


info() {
   echo "System Information:"
   echo "****************************"
   echo `head -n 1 /etc/issue`
   echo `uname -a`
   echo
   echo "JAVA_HOME=$JAVA_HOME"
   echo `$JAVA_HOME/bin/java -version`
   echo
   echo "APP_HOME=$APP_HOME"
   echo "APP_MAINCLASS=$APP_MAINCLASS"
   echo "****************************"
}

dump(){
    checkpid

    if [ $psid -ne 0 ];  then
        echo -e "Dumping the $APP_MAINCLASS ...\c"

        DUMP_DIR=$LOGS_DIR/dump
        if [ ! -d $DUMP_DIR ]; then
            mkdir $DUMP_DIR
        fi
        DUMP_DATE=`date +%Y%m%d%H%M%S`
        DATE_DIR=$DUMP_DIR/$DUMP_DATE
        if [ ! -d $DATE_DIR ]; then
            mkdir $DATE_DIR
        fi

        for PID in $psid ; do
            $JAVA_HOME/bin/jstack $psid > $DATE_DIR/jstack-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jinfo $psid > $DATE_DIR/jinfo-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jstat -gcutil $psid > $DATE_DIR/jstat-gcutil-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jstat -gccapacity $psid > $DATE_DIR/jstat-gccapacity-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jmap $psid > $DATE_DIR/jmap-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jmap -heap $psid > $DATE_DIR/jmap-heap-$psid.dump 2>&1
            echo -e ".\c"
            $JAVA_HOME/bin/jmap -histo $psid > $DATE_DIR/jmap-histo-$psid.dump 2>&1
            echo -e ".\c"
            if [ -r /usr/sbin/lsof ]; then
            /usr/sbin/lsof -p $psid > $DATE_DIR/lsof-$psid.dump
            echo -e ".\c"
            fi
        done

        if [ -r /bin/netstat ]; then
        /bin/netstat -an > $DATE_DIR/netstat.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/iostat ]; then
        /usr/bin/iostat > $DATE_DIR/iostat.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/mpstat ]; then
        /usr/bin/mpstat > $DATE_DIR/mpstat.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/vmstat ]; then
        /usr/bin/vmstat > $DATE_DIR/vmstat.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/free ]; then
        /usr/bin/free -t > $DATE_DIR/free.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/sar ]; then
        /usr/bin/sar > $DATE_DIR/sar.dump 2>&1
        echo -e ".\c"
        fi
        if [ -r /usr/bin/uptime ]; then
        /usr/bin/uptime > $DATE_DIR/uptime.dump 2>&1
        echo -e ".\c"
        fi
        echo "OK!"
        echo "DUMP: $DATE_DIR"
    else
        echo "$APP_MAINCLASS is not running"
    fi
}


case "$1" in
   'start')
      start
      ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     ;;
   'status')
     status
     ;;
   'info')
     info
     ;;
   'dump')
     dump
     ;;
   *)
     echo "Usage: $0 {start|stop|restart|status|info}"
     #exit 1
     ;;
esac
exit 0
