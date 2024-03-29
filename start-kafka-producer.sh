#!/bin/sh

# Strating Kafka connect

echo -e "Current file contents:\n $(cat /etc/hosts)"
echo "$DETECTED_IP $DETECTED_HOSTNAME" >> /etc/hosts
echo -e "\n\n\nUpdated file contents:\n $(cat /etc/hosts)"

echo $BROKERS, $BATCHSIZE, $LINGERMS, $BUFFERMEMORY
sed -i "s/BROKERS/${BROKERS}/g" /opt/producer.properties
sed -i "s/BATCHSIZE/${BATCHSIZE}/g" /opt/producer.properties
sed -i "s/LINGERMS/${LINGERMS}/g" /opt/producer.properties
sed -i "s/BUFFERMEMORY/${BUFFERMEMORY}/g" /opt/producer.properties

echo Starting Kafka producer

cd /opt

EXTRA_ARGS=-javaagent:/opt/jmx_prometheus_javaagent-0.13.0.jar=3800:/opt/kafka-producer-consumer.yml
java $EXTRA_ARGS -jar KafkaClickstreamClient-1.0-SNAPSHOT.jar -t $TOPIC -pfp /opt/producer.properties -nd -nt $NMTRD -nle -gsr -gsrr $REGION -iam -gar -gcs FULL_ALL
