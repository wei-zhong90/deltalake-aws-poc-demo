#!/bin/bash

./kafka-topics.sh --create \
    --bootstrap-server b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic lego

./kafka-console-consumer.sh --bootstrap-server b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092 \
    --consumer.config client.properties \
    --topic lego --from-beginning