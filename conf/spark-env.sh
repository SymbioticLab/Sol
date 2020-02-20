#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
SPARK_JAVA_OPTS+="-Dspark.local.dir=/mnt/tmp -Dhadoop.tmp.dir=/mnt/tmp"
export SPARK_JAVA_OPTS
SPARK_LOCAL_DIRS=/mnt/tmp
export SPARK_LOCAL_DIRS
SPARK_WORKER_DIR=/mnt/tmp
export SPARK_WORKER_DIR
SPARK_LOCAL_IP=100.0.0.253
#dc3master-lan2
SPARK_MASTER_HOST=dc3master-lan2
HADOOP_CONF_DIR=/users/jimmyyou/hadoop-2.8.4/etc/hadoop
YARN_CONF_DIR=/users/jimmyyou/hadoop-2.8.4/etc/hadoop
HIVE_HOME=/users/jimmyyou/apache-hive-2.0.0-bin
