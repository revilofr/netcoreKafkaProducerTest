# OVERALL procedure to check kafka

## install kafka on your dev machine
https://kafka.apache.org/quickstart

## Start a kafka terminal and execute the following lines
cd Docker_Context
./start_kafka.sh with chmod u+x start_kafka.sh


## Start a producer terminal and execute the following line
bin/kafka-console-producer.sh --broker-list $DOCKER_HOST_IP:9092 --topic witsmlCreateEvent


## Start a consumer terminal and execute the following line
bin/kafka-console-consumer.sh --bootstrap-server $DOCKER_HOST_IP:9092 --topic witsmlCreateEvent --from-beginning
