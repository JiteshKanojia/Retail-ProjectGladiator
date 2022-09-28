#!/bin/bash

if [ $# -ne 2 ];
then
  printf "script takes 2 args [1] port number , [2] topic name \nbye \n"
  exit 1
fi

#Start Zookeper instance
printf "[+] Starting Zookeeper...Please Wait \n"
~/kafka_2.12-3.0.0/bin/zookeeper-server-start.sh  ~/kafka_2.12-3.0.0/config/zookeeper.properties </dev/null &>/dev/null & sleep 5
printf "[+] Zookeeper Started \n"

#Start kafka server instance by running
printf "[+] Starting Kafka \n"
~/kafka_2.12-3.0.0/bin/kafka-server-start.sh ~/kafka_2.12-3.0.0/config/server.properties </dev/null &>/dev/null & sleep 5
printf "[+] Kafka Started \n"

#Create a Kafka topic
printf "[+] Creating kafka topic \n"
~/kafka_2.12-3.0.0/bin/kafka-topics.sh  --create --bootstrap-server localhost:$1 --replication-factor 1 --partitions 1 --topic $2 
printf "[+] Topic %s Created \n" $2

#Create a producter in console to publish to data streama
printf "[+] Creating producer on localhost:%d \n" $1
~/kafka_2.12-3.0.0/bin/kafka-console-producer.sh --bootstrap-server localhost:$1 --topic $2

