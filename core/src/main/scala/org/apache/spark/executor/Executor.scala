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

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent._
import java.util.concurrent.atomic._
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map, Queue, HashSet, ListBuffer}
import scala.util.control.NonFatal
import scala.util.Random
import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, Task, TaskDescription}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.storage._
import org.apache.spark.ShuffledRowRDDPartition
import org.apache.spark.scheduler.{ShuffleMapTask, ResultTask}

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    allocatedCores: Int = 0,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  private val startTime = System.currentTimeMillis

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname)
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // Start worker thread pool
  private val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread =
          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
          // will hang forever if some methods are interrupted.
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  private val executorSource = new ExecutorSource(threadPool, executorId)
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()
  private val random = Random

  // For testing only
  if (!isLocal) {
    env.blockManager.initialize(conf.getAppId)
    env.metricsSystem.registerSource(executorSource)
    env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
    random.setSeed(233)
  }

  env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerLocalAddress(env.blockManager.blockManagerId)
  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)
  // SPARK-21928.  SerializerManager's internal instance of Kryo might get used in netty threads
  // for fetching remote cached RDD blocks, so need to make sure it uses the right classloader too.
  env.serializerManager.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  private val maxResultSize = conf.get(MAX_RESULT_SIZE)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Maintains the list of the start time of queuing tasks 
  private val queuingTime = new HashMap[Long, Long]()

  private val failedTask = new ConcurrentHashMap[Long, Boolean]()

  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  // whether to use the Parallel Containers, tasks are ready
  private val readyContext = new HashMap[Long, ExecutorBackend]()
  private val readyTaskDes = new HashMap[Long, TaskDescription]()

  // tasks have not available yet
  private val waitingContext = new HashMap[Long, ExecutorBackend]()
  private val waitingTaskDes = new HashMap[Long, TaskDescription]()

  private val taskParentInfo = new ConcurrentHashMap[Long, String]()
  private val taskReceiveTime = new ConcurrentHashMap[Long, Long]()

  private val waitingShuffle = new HashMap[String, Boolean]()
  private val fetchedShuffle = new HashMap[String, Boolean]()

  // tasks in fetching shuffle 
  private val tasksInFetching = new ConcurrentHashMap[Long, Boolean]()
  private val realTaskInDecoupling = new ConcurrentHashMap[Long, Boolean]()

  private val availCores = allocatedCores match{
    case 0 => new AtomicInteger(conf.getInt("spark.executor.cores", 1))
    case _ => new AtomicInteger(allocatedCores)
  } 

  private val decouplingLock = new AtomicInteger(0)

  private val BROADCAST_THRESHOLD = conf.getInt("spark.broadcast.threshold", 1 << 19) // 512K

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  private val NUM_COMM_TASK_DECOUPLING = conf.getInt("spark.communicationtasks.num", 1)

  private val INDEPEND_DECOUPLING = conf.getBoolean("spark.decoupling.enabled", false)

  private val ENABLE_DECOUPLING = (conf.getBoolean("spark.decouplingExecutor.enabled", false) || INDEPEND_DECOUPLING)

  private val ENABLE_SMALL_BROADCAST = conf.getBoolean("spark.smallBroadcast.enabled", false)

  private val BROADCAST_FILES = conf.getBoolean("spark.broadcastFiles.enabled", true)

  private val PROACTIVE_PUSH_ENABLED = conf.getBoolean("spark.proactivePushEnable.enabled", false)

  private val PC_ENABLE = conf.getBoolean("spark.pipelining.enabled", false)

  private val TASK_FAILURE_RATE = conf.getOption("spark.taskfailure.rate") match {
    case Some(rate) => rate.toDouble
    case _ => 0D
  }

  private val TOLERANCE_TEST = conf.getBoolean("spark.localRestart.enabled", false)

  private val isFairEnabled: Boolean = conf.getOption("spark.scheduler.mode") match {
    case Some(mode) => 
      if (mode == "FAIR") {
        true
      } else {
        false
      }
      
    case _ => false
  }

  private val stageInRunning = new HashMap[Int, Int]()

  // for FAIR scheduling 
  private val taskList = new ListBuffer[TaskDescription]()

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  private var exeBackend: ExecutorBackend = null
  
  // Executor for the monitor task.
  private val taskMonitorPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-taskMonitor")

  if (ENABLE_DECOUPLING) {
    startTaskMonitor()
  }

  startDriverHeartbeater()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val launchStart = System.currentTimeMillis

    if (exeBackend == null) {
      exeBackend = context
    }

    val totalTask = waitingContext.size + readyTaskDes.size + runningTasks.size + 1
    
    if (isFairEnabled) {
      taskList += taskDescription
    }

    taskReceiveTime.put(taskDescription.taskId, launchStart)
    queuePush(context, taskDescription)
    localLaunch()

    val launchTime = System.currentTimeMillis - launchStart
  }

  def addPeers(peersAdd: String) {
    if (peersAdd.length > 0) {
      env.blockManager.blockTransferService.asInstanceOf[NettyBlockTransferService].addPeers(peersAdd)
    }
  }

  def getBloackManagerPort(): String = {
    env.blockManager.blockTransferService.asInstanceOf[NettyBlockTransferService].port.toString
  }

  def registerMapStatus(shuffleId: Int, mapStatus: Array[Byte]): Unit = {
    env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerMapStatus(shuffleId, mapStatus)
  }

  def localLaunch(): Unit = synchronized {
    // update the statuses of tasks in waitQueue
    updateTaskStatus()

    while (availCores.get() > 0 && readyContext.size > 0) {
      val (nextContext, nextTaskDes) = queuePull()

      if (nextContext == null) {
        return 
      }

      availCores.decrementAndGet()
      val tr = new TaskRunner(nextContext, nextTaskDes)
      runningTasks.put(nextTaskDes.taskId, tr)
      threadPool.execute(tr)
    }
  }

  def readyPush(context: ExecutorBackend, taskDescription: TaskDescription): Unit = synchronized {
    val taskId = taskDescription.taskId
    
    readyContext.put(taskId, context)
    readyTaskDes.put(taskId, taskDescription)
  }

  def waitPush(context: ExecutorBackend, taskDescription: TaskDescription): Unit = synchronized {
    val taskId = taskDescription.taskId
    waitingContext.put(taskId, context)
    waitingTaskDes.put(taskId, taskDescription)
  }

  def addMoreCores(cores: Int): Unit = {
    availCores.addAndGet(cores)
  }

  def queuePush(context: ExecutorBackend, taskDescription: TaskDescription): Unit = synchronized {
    val taskId = taskDescription.taskId
    queuingTime(taskId) = System.currentTimeMillis()

    if (taskDescription.shuffleInfo.length > 0 && ENABLE_DECOUPLING && !failedTask.containsKey(taskId)) {
      val shuffleInfo = taskDescription.shuffleInfo
      // shuffleMap tasks
      if (shuffleInfo.contains("@")) {
        val parents = shuffleInfo.split("@")

        // some dependencies
        parents.length match {
          case 1 => 
            readyPush(context, taskDescription)
          case _ => 
            // have dependency on shuffles 
            val depInfo = parents(1)
            if (!fetchedShuffle.contains(depInfo)) {
              waitingShuffle.put(depInfo, true)
              taskParentInfo.put(taskId, depInfo)
              waitPush(context, taskDescription)
            } else {
              readyPush(context, taskDescription)
            }
        }
      } else {
        if (!fetchedShuffle.contains(shuffleInfo)) {
          waitingShuffle.put(shuffleInfo, true)
          taskParentInfo.put(taskId, shuffleInfo)
          waitPush(context, taskDescription)
        } else {
          readyPush(context, taskDescription)
        }
      }
    } else {
      readyPush(context, taskDescription)
    }
  }

  def cleanUpWaitingTask(taskId: Long): Unit = synchronized {
    waitingContext.remove(taskId)
    waitingTaskDes.remove(taskId)
    tasksInFetching.remove(taskId)
  }

  // Have enabled decoupling at least
  def updateTaskStatus(): Unit = synchronized {
    if (waitingTaskDes.size > 0) {

      if (INDEPEND_DECOUPLING) {
        // Start decoupling for fetching the data in other threads
        // have collected all shuffleInfo

        var tasksIndecoupling: Int = 0

        val blocksToFetch = waitingTaskDes.filter { case (taskId, des) => 
          if (tasksInFetching.containsKey(taskId)){
            false
          } else {
            val parentShuffles = taskParentInfo.get(taskId).split("_")
            var recivedAllInfo: Int = 1
            for (parent <- parentShuffles) 
              recivedAllInfo = math.min(recivedAllInfo, shuffleStatusReady(parent.toInt, des.partitionId))

            (recivedAllInfo != 0)
          }
        }

        // If blocksToFetch is not None, then fetch the blocks in advance
        if (blocksToFetch.nonEmpty) {
          blocksToFetch.map {
            case (taskId, des) =>
              tasksInFetching.put(taskId, true)

              val reduceId = des.partitionId
              // Impossible to be NULL
              val parentShuffles = taskParentInfo.get(taskId).split("_")
              val parentMap = new HashMap[Int, Int]

              val totalPartitions = parentShuffles.map { case parent => 
                val pId = parent.toInt
                val parentPartition = env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].getShufflePartitions(pId)
                parentMap.put(pId, parentPartition)
                parentPartition
              }.sum
              
              val requireFetch = env.blockManager.fetchBlocks(taskId, totalPartitions, parentMap, reduceId)

              if (requireFetch) {
                realTaskInDecoupling.put(taskId, true)
                tasksIndecoupling += 1
              }
          }
        }

        // check whether all the parent shuffleInfo has been received or not
        val updateIndex = waitingTaskDes.filter { case (taskId, des) => taskReady(taskId) == true }

        if (updateIndex.nonEmpty) {
          updateIndex.map {
            case (taskId, des) =>
              val taskCon = waitingContext(taskId)
              readyPush(taskCon, des)
              cleanUpWaitingTask(taskId)

              if (realTaskInDecoupling.containsKey(taskId)) {
                tasksIndecoupling -= 1
                realTaskInDecoupling.remove(taskId)
              }
          }
        }

        if (tasksInFetching.size() > 0 && decouplingLock.get() == 0) {
          // decoupling begins, then reserve cpus for decoupling 
          tasksIndecoupling -= NUM_COMM_TASK_DECOUPLING
          decouplingLock.decrementAndGet()
        } else if (tasksInFetching.size() == 0 && decouplingLock.get() == -1) {
          // decoupling completes for all tasks, thus releasing communication tasks
          tasksIndecoupling += NUM_COMM_TASK_DECOUPLING
          decouplingLock.incrementAndGet()
        }

        // Release more or acquire cpus
        if (tasksIndecoupling != 0) {
          // notify the master to schedule more tasks
          exeBackend.statusUpdate(-1, TaskState.RUNNING, EMPTY_BYTE_BUFFER, tasksIndecoupling.toString + "#", readyContext.size, waitingContext.size)
        }
        
      } else {
        waitingShuffle.map { case (shuffleDep, waiting) =>
            val parentShuffles = shuffleDep.split("_")
            var receivedAllInfo: Int = 1
            parentShuffles.map {case parent => receivedAllInfo = math.min(receivedAllInfo, shuffleStatusReady(parent.toInt))}

            if (receivedAllInfo != 0) {
              waitingShuffle.remove(shuffleDep)
              fetchedShuffle.put(shuffleDep, true)

              waitingTaskDes.map {
                case (taskId, des) =>
                  if (taskParentInfo.get(taskId) == shuffleDep) {
                    val taskCon = waitingContext(taskId)
                    readyPush(taskCon, des)
                    cleanUpWaitingTask(taskId)
                  }
              }
            }
        }
      }
    }
  }

  def selectFeasibleTaskFAIR(): Long = {
    taskList.foreach {
      case taskdes =>
        if (readyTaskDes.contains(taskdes.taskId)) {
          taskList -= taskdes
          return taskdes.taskId
        }
    }
    
    return 0L
  }

  def queuePull(): (ExecutorBackend, TaskDescription) = synchronized {
    var minValue = scala.Long.MaxValue
    var minStage = scala.Long.MaxValue
    var selectedTaskId = 0L

    for ((taskId, taskdes) <- readyTaskDes) {
      if (minValue > taskdes.priority || (minValue == taskdes.priority && minStage > taskdes.attemptOrder)) {
        minValue = taskdes.priority
        minStage = taskdes.attemptOrder
        selectedTaskId = taskId
      }
    }

    val context = readyContext(selectedTaskId)
    val taskDescription = readyTaskDes(selectedTaskId)

    readyContext.remove(selectedTaskId)
    readyTaskDes.remove(selectedTaskId)

    queuingTime(taskDescription.taskId) = System.currentTimeMillis() - queuingTime(taskDescription.taskId)

    (context, taskDescription)
  }

  def taskReady(taskId: Long): Boolean = {
    env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].getStatusForTask(taskId)
  }

  def shuffleStatusReady(pId: Int, reduceId: Int = -1): Int = {
    env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].getRunStatus(pId, reduceId)
  }

  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def handleSiteFailure(): Boolean = synchronized {
    // no site manager 
    if (!PC_ENABLE) {
      return true
    }
    if (waitingContext.size + readyTaskDes.size == 0) {
      return false
    }
    val beforeTasks = waitingContext.size

    // remove tasks in the pending queue, the ready task should have already been allocated
    waitingTaskDes.map{
      case (taskId, des) =>
        waitingTaskDes.remove(taskId)
        waitingContext.remove(taskId)

        //killTask(taskId, interruptThread = true, reason = reason)
        //not being exactly executed 
        exeBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER, "", 0, 0)
        val ser = env.closureSerializer.newInstance()
        val serializedTK = ser.serialize(TaskKilled("KILL_TASK_TEST"))
        exeBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
    }

    // if not infetching, then also remove 
    val beforeSizeReady = readyTaskDes.size
    readyTaskDes.map{
      case (taskId, des) =>
        if (!tasksInFetching.containsKey(taskId)) {
          readyTaskDes.remove(taskId)
          readyContext.remove(taskId)

          //killTask(taskId, interruptThread = true, reason = reason)
          //not being exactly executed 
          exeBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER, "", 0, 0)
          val ser = env.closureSerializer.newInstance()
          val serializedTK = ser.serialize(TaskKilled("KILL_TASK_TEST"))
          exeBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
        }
    }

    true
  }

  def killTasksT(cores: Int, interruptThread: Boolean, reason: String, lostCores: Boolean = false) : Boolean = synchronized {
    if (runningTasks.size == 0) {
      return false
    }
    var count: Int = 0
    runningTasks.keys().asScala.foreach(t =>
      if (count < cores) {
        killTask(t, interruptThread = interruptThread, reason = reason)
        count += 1
      }
    )
    // reduce # of availCores
    val beforeCores = availCores.get

    val nowCores = lostCores match {
      case true => availCores.addAndGet(-cores)
      case _ => beforeCores
    }
  
    true
  }

  def killRunningTask(tasksToKill: Int): Unit = {
    var count: Int = 0
    runningTasks.keys().asScala.foreach (
      t =>
        if (count < tasksToKill && !failedTask.containsKey(t)) {
          failedTask.put(t, true)
          killTask(t, interruptThread = true, reason = "KILL_TASK_TEST")
          count += 1
        }
    )
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    taskMonitorPool.shutdown()
    taskMonitorPool.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  class TaskRunner(
      execBackend: ExecutorBackend,
      private val taskDescription: TaskDescription)
    extends Runnable {

    val taskId = taskDescription.taskId

    var parentInfo = taskParentInfo.get(taskId)

    val shuffleInfo = taskDescription.shuffleInfo.contains('@') match {
      case true => taskDescription.shuffleInfo.split('@')(0)
      case _ => null
    }


    val threadName = s"Executor task launch worker for task $taskId"
    private val taskName = taskDescription.name

    /** If specified, this task has been killed and this option contains the reason. */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId), reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    /**
     *  Utility function to:
     *    1. Report executor runtime and JVM gc time if possible
     *    2. Collect accumulator updates
     *    3. Set the finished flag to true and clear current thread's interrupt status
     */
    private def collectAccumulatorsAndResetStatusOnFailure(taskStartTime: Long) = {
      // Report executor runtime and JVM gc time
      Option(task).foreach(t => {
        t.metrics.setExecutorRunTime(System.currentTimeMillis() - taskStartTime)
        t.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
      })

      // Collect latest accumulator values to report back to the driver
      val accums: Seq[AccumulatorV2[_, _]] =
        Option(task).map(_.collectAccumulatorUpdates(taskFailed = true)).getOrElse(Seq.empty)
      val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

      setTaskFinishedAndClearInterruptStatus()
      (accums, accUpdates)
    }

    override def run(): Unit = {
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      var TEST_LOCAL_RESTART: Boolean = false
      val threadMXBean = ManagementFactory.getThreadMXBean
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()

      val piggBack = (availCores.get).toString
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER, piggBack, readyContext.size, waitingContext.size)

      var taskStartTime: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()

      try {
        // Must be set before updateDependencies() is called, in case fetching dependencies
        // requires access to properties contained within (e.g. for access control).
        Executor.taskDeserializationProps.set(taskDescription.properties)

        val updateDepTime = System.currentTimeMillis()
        updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)

        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)

        val eplipseDes = System.currentTimeMillis() - updateDepTime

        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException(killReason.get)
        }

        // The purpose of updating the epoch here is to invalidate executor map output status cache
        // in case FetchFailures have occurred. In local mode `env.mapOutputTracker` will be
        // MapOutputTrackerMaster and its cache invalidation is not based on epoch numbers so
        // we don't need to make any special calls here.
        if (!isLocal) {
          logDebug("Task " + taskId + "'s epoch is " + task.epoch)
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        // Run the actual task and measure its runtime.
        taskStartTime = System.currentTimeMillis()
        var taskDoneRunTime: Long = 0

        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true

        val value = try {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          taskDoneRunTime = System.currentTimeMillis

          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
        task.context.fetchFailed.foreach { fetchFailure =>
          // uh-oh.  it appears the user code has caught the fetch-failure without throwing any
          // other exceptions.  Its *possible* this is what the user meant to do (though highly
          // unlikely).  So we will log an error and keep going.
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure)
        }
        val taskFinish = System.currentTimeMillis()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // If the task has been killed, let's fail it.
        task.context.killTaskIfInterrupted()

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()
        val executorDeserializeTime = task.executorDeserializeTime
        val taskDoneTime = taskDoneRunTime - taskStartTime
        val metricsDoneTime = (taskFinish - taskStartTime) - task.executorDeserializeTime

        val executorDeserializeCpuTime = task.executorDeserializeCpuTime
        // Deserialization happens in two parts: first, we deserialize a Task object, which
        // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
        
        task.metrics.setExecutorDeserializeTime(
          (taskStartTime - deserializeStartTime) + task.executorDeserializeTime)
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // We need to subtract Task.run()'s deserialization time to avoid double-counting
        task.metrics.setExecutorRunTime((taskFinish - taskStartTime) - task.executorDeserializeTime)
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)
        task.metrics.setExecutorQueuingTime(queuingTime(taskId))
        // Expose task metrics using the Dropwizard metrics system.
        // Update task metrics counters

        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
          .inc(task.metrics.shuffleReadMetrics.fetchWaitTime)
        executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(task.metrics.shuffleWriteMetrics.writeTime)
        executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.totalBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.remoteBytesRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
          .inc(task.metrics.shuffleReadMetrics.remoteBytesReadToDisk)
        executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
          .inc(task.metrics.shuffleReadMetrics.localBytesRead)
        executorSource.METRIC_SHUFFLE_RECORDS_READ
          .inc(task.metrics.shuffleReadMetrics.recordsRead)
        executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.remoteBlocksFetched)
        executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
          .inc(task.metrics.shuffleReadMetrics.localBlocksFetched)
        executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.bytesWritten)
        executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
          .inc(task.metrics.shuffleWriteMetrics.recordsWritten)
        executorSource.METRIC_INPUT_BYTES_READ
          .inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN
          .inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = task.collectAccumulatorUpdates()
        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit()

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logInfo(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            val taskFinishTime = System.currentTimeMillis
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver, startTime ${taskStartTime}, finishTime ${taskFinishTime}")
            serializedDirectResult
          }
        }

        setTaskFinishedAndClearInterruptStatus()
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult, taskReceiveTime.get(taskId).toString + "@" + System.currentTimeMillis.toString)

        if (shuffleInfo != null) {
          val shuffleId = shuffleInfo.split('_')(0).toInt
          val numPartitions = shuffleInfo.split('_')(1).toInt
          val mapId = task.partitionId
          // Get the size of data file
          val outputSize = value.asInstanceOf[MapStatus].getSize
          val bytesWritten = task.metrics.shuffleWriteMetrics.bytesWritten

          // MapStatus: location: BlockManagerId, getSizeForBlock(reduceId: Int): Long
          // registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus)
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerMapOutput(shuffleId, mapId, value.asInstanceOf[MapStatus], numPartitions, outputSize, true)
          

          // [Outdated] If shuffleMap tasks, then store the shuffleInfo locally for broadcast
          if (ENABLE_SMALL_BROADCAST) {
            val updateShuffleTime = System.currentTimeMillis()

            val accumulatedBytes = env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].getAccumulatedSize(shuffleId)
            val estimatedBytes = env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].getEstimatedSize(shuffleId)
            
            if (BROADCAST_FILES) {
              // broadcast the mapoutput of the task
              var info = numPartitions.toString + "@" + System.currentTimeMillis.toString + "@" + executorId
              if (outputSize > BROADCAST_THRESHOLD || outputSize == 0)
                info = "#" + info

              // mightbe some cases where have zero output, but we still need to broadcast the mapStatus
              val shuffleIndexFile = ShuffleIndexBlockId(shuffleId, mapId, 0)
              val shuffleDataFile = ShuffleDataBlockId(shuffleId, mapId, 0)

              env.blockManager.broadcastData(shuffleIndexFile, shuffleDataFile, value.asInstanceOf[MapStatus], info)
            } else {
              env.blockManager.broadcastBlockData(ShuffleBlockId(shuffleId, mapId, 0), value.asInstanceOf[MapStatus], numPartitions)
            }

            val updateShuffledelta = System.currentTimeMillis() - updateShuffleTime
          }

          if (PROACTIVE_PUSH_ENABLED) {
            // if pending child task has been found, then forward
            env.blockManager.pushOutputRequest(shuffleId.toString, task.partitionId)
          }
        }
      } catch {
        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)
          val serializedTK = ser.serialize(TaskKilled(t.reason, accUpdates, accums))
          if (t.reason == "KILL_TASK_TEST" && TOLERANCE_TEST) {
            failedTask.put(taskId, true)
            TEST_LOCAL_RESTART = true
          } else {
            execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
          }

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)
          val serializedTK = ser.serialize(TaskKilled(killReason, accUpdates, accums))
          
          if (killReason == "KILL_TASK_TEST" && TOLERANCE_TEST) {
            failedTask.put(taskId, true)
            TEST_LOCAL_RESTART = true
          } else {
            execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
          }

        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // SPARK-20904: Do not report failure to driver if if happened during shut down. Because
          // libraries may set up shutdown hooks that race with running tasks during shutdown,
          // spurious failures may occur and can result in improper accounting in the driver (e.g.
          // the task failure would not be ignored if the shutdown happened because of premption,
          // instead of an app issue).
          if (!ShutdownHookManager.inShutdown()) {
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTime)

            val serializedTaskEndReason = {
              try {
                ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (!t.isInstanceOf[SparkOutOfMemoryError] && Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        runningTasks.remove(taskId)
        queuingTime.remove(taskId)
        taskParentInfo.remove(taskId)
        taskReceiveTime.remove(taskId)

        // this task is killed deliberately
        if (TEST_LOCAL_RESTART) {
          execBackend.statusUpdate(taskId, TaskState.KILLED, null, "-1")
        } else if (random.nextDouble() < TASK_FAILURE_RATE) {
          killRunningTask(1)
        }

        availCores.incrementAndGet()
        localLaunch()

        // logInfo (s"====At time ${System.currentTimeMillis}, Done (TID:$taskId), now ${waitingTaskDes.size} in waitinglist, ${readyContext.size} in ready, ${runningTasks.size} in running, ${availCores.get} available cores")
      }
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }

  /**
   * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
   * sending a Thread.interrupt(), and monitoring the task until it finishes.
   *
   * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
   * may not be interruptable or may not respond to their "killed" flags being set. If a significant
   * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
   * remain running then this can lead to a situation where new jobs and tasks are starved of
   * resources that are being used by these zombie tasks.
   *
   * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
   * tasks. For backwards-compatibility / backportability this component is disabled by default
   * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
   *
   * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
   * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
   * in case kill is called twice with different values for the `interrupt` parameter.
   *
   * Once created, a TaskReaper will run until its supervised task has finished running. If the
   * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
   * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
   * if the supervised task never exits.
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()
      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs
      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * Monitor the waiting task and ready task.
   */
  private def startTaskMonitor(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.taskMonitorInterval", "50ms")
    val monitor = new Runnable() {
      override def run(): Unit = {
        localLaunch()
      }
    }

    taskMonitorPool.scheduleAtFixedRate(monitor, 0, intervalMs, TimeUnit.MILLISECONDS)
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}

