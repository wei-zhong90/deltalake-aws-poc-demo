#!/bin/bash

./kafka-topics.sh --create \
    --bootstrap-server b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic lego

./kafka-console-consumer.sh --bootstrap-server b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092 \
    --consumer.config client.properties \
    --topic lego --from-beginning


./kafka-topics.sh --delete \
    --bootstrap-server b-1.streaming-data-so.ehfjpu.c4.kafka.ap-northeast-1.amazonaws.com:9094,b-2.streaming-data-so.ehfjpu.c4.kafka.ap-northeast-1.amazonaws.com:9094 \
    --command-config client.properties \
    --topic test2