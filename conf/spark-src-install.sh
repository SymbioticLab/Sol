#!/usr/bin/env bash

cd ~
git clone https://github.com/apache/spark.git
cd spark
./build/mvn -DskipTests -Phive -Phive-thriftserver -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 package -T 56

export SPARK_HOME=/users/jimmyyou/spark
export PATH=$PATH:$SPARK_HOME/bin

grep -o -E 'h[0-9]+$' /etc/hosts > $SPARK_HOME/conf/slaves
cp ~/hadoop-helper/spark* $SPARK_HOME/conf/
cp ~/hadoop-helper/hive-site.xml $SPARK_HOME/conf/ 
