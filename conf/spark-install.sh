#!/usr/bin/env bash

cd ~
wget http://apache.osuosl.org/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz 
tar -xzf spark-2.3.1-bin-hadoop2.7.tgz

export SPARK_HOME=/users/jimmyyou/spark-2.3.1-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin

grep -o -E 'h[0-9]+$' /etc/hosts > $SPARK_HOME/conf/slaves
cp ~/hadoop-helper/spark* $SPARK_HOME/conf/
cp ~/hadoop-helper/hive-site.xml $SPARK_HOME/conf/ 
