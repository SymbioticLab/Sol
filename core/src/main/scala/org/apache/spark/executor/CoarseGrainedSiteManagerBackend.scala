/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import scala.collection.mutable.HashMap
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.scheduler.cluster._

private[spark] class CoarseGrainedSiteManagerBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    hostname: String,
    siteManagerPort: Int
  )
  extends ThreadSafeRpcEndpoint with Logging {

  private[this] val stopping = new AtomicBoolean(false)

  var transferServiceAdd = ""

  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  //private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  private[this] val runningTasks = new ConcurrentHashMap[Long, TaskDescription]

  private[this] val executorDataMap = new HashMap[String, ExecutorData]

  private[this] val testInfo = new HashMap[Long, String]

  private[this] val sysStartTime = System.currentTimeMillis


  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    // rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
    //   // This is a very fast action so we can use "ThreadUtils.sameThread"
    //   driver = Some(ref)
    //   ref.ask[Boolean](RegisterSiteManager(hostname))
    // }(ThreadUtils.sameThread).onComplete {
    //   // This is a very fast action so we can use "ThreadUtils.sameThread"
    //   case Success(msg) =>
    //     // Always receive `true`. Just ignore it
    //     logInfo(s"====Successfully set up site Manager")
    //   case Failure(e) =>
    //     logInfo(s"====Failed to set up site Manager as ${e}")
    //     //exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    // }(ThreadUtils.sameThread)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterInferior(executorId, executorRef, hostname, core, logUrls) =>
      if (executorDataMap.contains(executorId)) {
        logInfo(s"====Error: register the same inferior again")
      }
      else {
        logInfo(s"====RegisterInferior for $executorRef")
        val executorAddress = if (executorRef.address != null) {
            executorRef.address
          } else {
            context.senderAddress
          }

        val data = new ExecutorData(executorRef, executorAddress, hostname,
            core, core, logUrls)
        executorDataMap.put(executorId, data)
      }
      context.reply(true)
  }

  override def receive: PartialFunction[Any, Unit] = {
    
    case AppendTestInfo(testinfo) =>
      logInfo(s"====AppendTestInfo ${testinfo}")
      val testItems = testinfo.split("@")
      testItems.map{
        case info =>
          val getTime = info.split("_")(0).toLong
          val getInfo = info.split("_")(1)
          testInfo.put(getTime, getInfo)
          logInfo(s"====Receive info ${info}")
      }

    case LaunchTask(data) =>
      // if (executor == null) {
      //   exitExecutor(1, "Received LaunchTask command but executor was null")
      // } else {
      //   val taskDesc = TaskDescription.decode(data.value)
      //   logInfo("Got assigned task " + taskDesc.taskId)
      //   executor.launchTask(this, taskDesc)
      //   runningTasks.put(taskDesc.taskId, taskDesc)
      // }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    }
  }
}
