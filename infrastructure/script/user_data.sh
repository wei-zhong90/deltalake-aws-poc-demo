#!/bin/bash

yum update -y

yum install java-1.8.0-openjdk -y

cd /home/ec2-user
wget https://archive.apache.org/dist/kafka/3.0.1/kafka-3.0.1-src.tgz
tar -xzf kafka-3.0.1-src.tgz
rm -f kafka-3.0.1-src.tgz