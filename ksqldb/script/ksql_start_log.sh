log_suffix=`date +"%Y%m%d%H%M%S"`
$CONFLUENT_HOME/bin/ksql-server-start $CONFLUENT_HOME/etc/ksqldb/ksql-server.properties 2>&1 | tee -a ~/ksql_console_log/ksql_console_$log_suffix.log