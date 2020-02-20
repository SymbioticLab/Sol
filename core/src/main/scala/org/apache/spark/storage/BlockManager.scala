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

package org.apache.spark.storage

import java.io._
import java.lang.ref.{ReferenceQueue => JReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{ReentrantLock, Lock, StampedLock}
import java.util.concurrent.atomic._

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.RecordReader

import com.codahale.metrics.{MetricRegistry, MetricSet}

import org.apache.spark._
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.netty._
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExternalShuffleClient, TempFileManager}
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.Serializer
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.storage._
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.network.shuffle.protocol.UploadBatchBlocks

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
                                  val data: Iterator[Any],
                                  val readMethod: DataReadMethod.Value,
                                  val bytes: Long)

/**
  * Abstracts away how blocks are stored and provides different ways to read the underlying block
  * data. Callers should call [[dispose()]] when they're done with the block.
  */
private[spark] trait BlockData {

  def toInputStream(): InputStream

  /**
    * Returns a Netty-friendly wrapper for the block's data.
    *
    * Please see `ManagedBuffer.convertToNetty()` for more details.
    */
  def toNetty(): Object

  def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer

  def toByteBuffer(): ByteBuffer

  def size: Long

  def dispose(): Unit

}

private[spark] class ByteBufferBlockData(
                                          val buffer: ChunkedByteBuffer,
                                          val shouldDispose: Boolean) extends BlockData {

  override def toInputStream(): InputStream = buffer.toInputStream(dispose = false)

  override def toNetty(): Object = buffer.toNetty

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    buffer.copy(allocator)
  }

  override def toByteBuffer(): ByteBuffer = buffer.toByteBuffer

  override def size: Long = buffer.size

  override def dispose(): Unit = {
    if (shouldDispose) {
      buffer.dispose()
    }
  }

}

/**
  * Manager running on every node (driver and executors) which provides interfaces for putting and
  * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
  *
  * Note that [[initialize()]] must be called before the BlockManager is usable.
  */
private[spark] class BlockManager(
   executorId: String,
   rpcEnv: RpcEnv,
   val master: BlockManagerMaster,
   val serializerManager: SerializerManager,
   val conf: SparkConf,
   memoryManager: MemoryManager,
   mapOutputTracker: MapOutputTracker,
   shuffleManager: ShuffleManager,
   val blockTransferService: BlockTransferService,
   securityManager: SecurityManager,
   numUsableCores: Int)
  extends BlockDataManager with BlockEvictionHandler with Logging {

  private[spark] val externalShuffleServiceEnabled =
    conf.getBoolean("spark.shuffle.service.enabled", false)

  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  // Visible for testing
  private[storage] val blockInfoManager = new BlockInfoManager

  private[spark] val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  private[spark] val futureCommunicationTaskPool = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("communication-task-pool", conf.getInt("spark.communicationtasks.num", 1)))


  // Actual storage of where blocks are kept
  private[spark] val memoryStore =
    new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager, securityManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory
  private val maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory

  private[this] val lock = new Object

  private[spark] val addedReader = new ConcurrentHashMap[String, RecordReader[Any, Any]]()

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager,
      securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT))
  } else {
    blockTransferService
  }

  private val BROADCAST_THRESHOLD = conf.getLong("spark.broadcast.threshold", 1 << 19)
  private val ENABLE_SMALL_BROADCAST = conf.getBoolean("spark.smallBroadcast.enabled", false)

  // threshold for decoupling, works on per reducer, 50M in default
  private var DECOUPLING_THRESHOLD = conf.getLong("spark.independentDecoupling.threshold", 50 * 1024 * 1024)

  private val PCENABLED = conf.getBoolean("spark.pipelining.enabled", false)

  private val ASYN_MASTER = (conf.getBoolean("spark.asynmaster.enabled", false) || PCENABLED)
  private val HTTP_BD = (conf.getBoolean("spark.httpbroadcast.enabled", false) || PCENABLED)

  private val PROACTIVE_PUSH_ENABLED = conf.getBoolean("spark.proactivePushEnable.enabled", false)

  private val PUSH_THRESHOLD = conf.getLong("spark.proactivePush.threshold", 100 * 1024)

  private val BATCH_SIZE = conf.getInt("spark.proactivePush.batchsize", 20)

  private val CENTRAL_TEST_SUITE = conf.getBoolean("spark.centralPush.enabled", false)
  private var startCentral: Boolean = false

  private val IN_MEMORY_SHUFFLE = conf.getBoolean("spark.inmemoryshuffle.enabled", true)

  private var decouplingThreshold = ENABLE_SMALL_BROADCAST match {
    case true => BROADCAST_THRESHOLD 
    case _ => 0
  }

  decouplingThreshold = PROACTIVE_PUSH_ENABLED match {
    case true => 
      if (decouplingThreshold > PUSH_THRESHOLD){
        decouplingThreshold
      }
      else {
        PUSH_THRESHOLD
      }
    case _ => decouplingThreshold
  }

  private val poisonValue = 999999

  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh =
    conf.getInt("spark.block.failures.beforeLocationRefresh", 5)

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L
  private val shuffleLock: Lock = new StampedLock().asWriteLock()

  private val fetchLock = new Object

  private var blockReplicationPolicy: BlockReplicationPolicy = _

  // check whether index file and data file have arrived
  private val shuffleFileIndicator = new ConcurrentHashMap[String, Boolean]

  private val fetchedBlocksTillNow = new ConcurrentHashMap[String, Int]

  // shuffleId, [reduceId, executorId]
  val shufflePendingRecord = new ConcurrentHashMap[String, HashMap[Int, String]]
  
  private val shufflePendingRequest = new AtomicInteger(0)
  private val getFirstLock = new AtomicInteger(0)

  // A TempFileManager used to track all the files of remote blocks which above the
  // specified memory threshold. Files will be deleted automatically based on weak reference.
  // Exposed for test
  private[storage] val remoteBlockTempFileManager =
  new BlockManager.RemoteBlockTempFileManager(this)
  private val maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)

  private var bytesRemoteRead = 0L
  private var blocksRemoteRead = 0
  private var isLocal: Boolean = true

  /**
    * Initializes the BlockManager with the given appId. This is not performed in the constructor as
    * the appId may not be known at BlockManager instantiation time (in particular for the driver,
    * where it is only learned after registration with the TaskScheduler).
    *
    * This method initializes the BlockTransferService and ShuffleClient, registers with the
    * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
    * service if configured.
    */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    shuffleClient.init(appId)

    blockReplicationPolicy = {
      val priorityClass = conf.get(
        "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }

    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

    val idFromMaster = master.registerBlockManager(
      id,
      maxOnHeapMemory,
      maxOffHeapMemory,
      slaveEndpoint)

    blockManagerId = if (idFromMaster != null) idFromMaster else id

    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }

    logInfo(s"Initialized BlockManager: $blockManagerId")
    isLocal = false
  }

  def shuffleMetricsSource: Source = {
    import BlockManager._

    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", shuffleClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", shuffleClient.shuffleMetrics())
    }
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
  }

  /**
    * Report all blocks to the BlockManager again. This may be necessary if we are dropped
    * by the BlockManager and come back or if we become capable of recovering blocks on disk after
    * an executor crash.
    *
    * This function deliberately fails silently if the master returns false (indicating that
    * the slave needs to re-register). The error condition will be detected again by the next
    * heart beat attempt or new block registration and another try to re-register all blocks
    * will be made then.
    */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
    * Re-register with the master and report all blocks to it. This will be called by the heart beat
    * thread if our heartbeat to the block manager indicates that we were not registered.
    *
    * Note that this method must be called without any BlockInfo locks held.
    */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
    * Re-register with the master sometime soon.
    */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
    * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
    */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
  }

  override def registerBroadcastMapStatus(shuffleBlockId: BlockId, mapStatus: MapStatus): Unit = {
    mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerBroadcastMapStatus(shuffleBlockId, mapStatus)
  }

  /**
    * Interface to get local block data. Throws an exception if the block cannot be found or
    * cannot be read successfully.
    */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          // If this block manager receives a request for a block that it doesn't have then it's
          // likely that the master has outdated block statuses for this block. Therefore, we send
          // an RPC so that this block is marked as being unavailable from this block manager.
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
    * Interface to get local block data. Throws an exception if the block cannot be found or
    * cannot be read successfully.
    */
  def getConvertBlockData(blockId: BlockId): ManagedBuffer = {
    // first check whether it is put in memoryStore
    shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver].getDirectBlockData(blockId.asInstanceOf[ShuffleBlockId])
  }

  /**
    * Interface to get local block data. Throws an exception if the block cannot be found or
    * cannot be read successfully.
    */
  def cachedInMemory(blockId: BlockId): Boolean = {
    // first check whether it is put in memoryStore
    IN_MEMORY_SHUFFLE && memoryStore.contains(blockId)
  }

  /**
    * Interface to get local block data. Throws an exception if the block cannot be found or
    * cannot be read successfully.
    */
  def getConvertBlockDataFromMemory(blockId: BlockId): ChunkedByteBuffer = {
    memoryStore.getBytes(blockId).get
  }

  /**
  * Put the block locally, using the given storage level.
  *
  * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
  * so may corrupt or change the data stored by the `BlockManager`.
  */
  override def putShuffleBlockDirectData[T: ClassTag](
     blockId: BlockId,
     data: ManagedBuffer,
     info: String
   ): Boolean = {

    val tLevel = StorageLevel.MEMORY_AND_DISK_SER
    val tellMaster = false

    val Infos = info.split("@")
    val transferTime = System.currentTimeMillis - Infos(0).toLong

    logInfo(s"====Try to put ShuffleBlock: ${blockId.name} with Info: ${info}, transfer from ${Infos(1)} takes ${transferTime} ms, data size ${data.size}")

    val result = putBytesShuffle(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), tLevel, tellMaster, true)(implicitly[ClassTag[T]])
    mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerPushBlockData(blockId)

    result
  }

  /**
  * Put the block locally, using the given storage level.
  *
  * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
  * so may corrupt or change the data stored by the `BlockManager`.
  */
  override def putShuffleBlockData[T: ClassTag](
     blockId: BlockId,
     data: ManagedBuffer,
     mapStatus: MapStatus,
     info: String
   ): Boolean = {

    val tLevel = StorageLevel.MEMORY_AND_DISK_SER
    val shuffle = blockId.name.replace(".data", "").replace(".index", "")
    val shuffleInfo = shuffle.split('_')
    val Infos = info.split("@")
    val blockInfo = Infos(0)

    val transferTime = System.currentTimeMillis - Infos(1).toLong

    logInfo(s"====Try to put ShuffleBlock: ${blockId.name} with Info: ${info}, transfer from ${Infos(2)} takes ${transferTime} ms, data size ${data.size}")

    // exceeds the threshold, then will only receive the index info
    if (blockInfo.contains("#")) {
      val numPartitions = blockInfo.replace("#","").toInt
      mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerMapOutput(shuffleInfo(1).toInt, shuffleInfo(2).toInt, mapStatus, numPartitions, -data.size, true)
      false
    } else {
      val numPartitions = blockInfo.toInt
      putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), tLevel, false, true)(implicitly[ClassTag[T]])

      // will receive both dataFile and indexFile
      shuffleFileIndicator.synchronized {
        if (shuffleFileIndicator.containsKey(shuffle)) {
          mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].registerMapOutput(shuffleInfo(1).toInt, shuffleInfo(2).toInt, mapStatus, numPartitions, data.size, true)
          shuffleFileIndicator.remove(shuffle)
        } else {
          shuffleFileIndicator.put(shuffle, true)
        }
      }
    }
  }

  /**
    * Put the block locally, using the given storage level.
    *
    * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
    * so may corrupt or change the data stored by the `BlockManager`.
    */
  override def putBlockData(
   blockId: BlockId,
   data: ManagedBuffer,
   level: StorageLevel,
   classTag: ClassTag[_]): Boolean = {
    putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
  }

  /**
    * Get the BlockStatus for the block identified by the given ID, if it exists.
    * NOTE: This is mainly for testing.
    */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
    * Get the BlockSize for the block identified by the given ID, if it exists.
    * NOTE: This is mainly for testing.
    */
  def getBlockSize(blockId: BlockId): Array[Long] = {
    val blockSize = new Array[Long](2)
    blockInfoManager.get(blockId).map { info =>
      blockSize(0) = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      blockSize(1) = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
    }

    blockSize
  }

  /**
    * Get the ids of existing blocks that match the given filter. Note that this will
    * query the blocks stored in the disk block manager (that the block manager
    * may not know of).
    */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
  }

  /**
    * Tell the master about the current storage status of a block. This will send a block update
    * message reflecting the current status, *not* the desired storage level in its block info.
    * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
    *
    * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
    * it is still valid). This ensures that update in master will compensate for the increase in
    * memory on slave.
    */
  private def reportBlockStatus( blockId: BlockId,
                                 status: BlockStatus,
                                 droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
    * Actually send a UpdateBlockInfo message. Returns the master's response,
    * which will be true if the block was successfully recorded and false if
    * the slave needs to re-register.
    */
  private def tryToReportBlockStatus(
                                      blockId: BlockId,
                                      status: BlockStatus,
                                      droppedMemorySize: Long = 0L): Boolean = {
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    val reportStart = System.currentTimeMillis
    //val result = master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
    if (ASYN_MASTER) {
      Future {
        master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
      } (futureExecutionContext)
    }
    else {
      master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
    }
    val reportTime = System.currentTimeMillis - reportStart
    // logInfo(s"====tryToReportBlockStatus for blockId ${blockId.name}, takes ${reportTime} ms")
    true
  }

  /**
    * Return the updated storage status of the block with the given ID. More specifically, if
    * the block is dropped from memory and possibly added to disk, return the new storage level
    * and the updated in-memory and on-disk sizes.
    */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
    * Get locations of an array of blocks.
    */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
    * Cleanup code run in response to a failed local read.
    * Must be called while holding a read lock on the block.
    */
  private def handleLocalReadFailure(blockId: BlockId): Nothing = {
    releaseLock(blockId)
    // Remove the missing block so that its unavailability is reported to the driver
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  /**
    * Get block from local block manager as an iterator of Java objects.
    */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId())
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          // We need to capture the current taskId in case the iterator completion is triggered
          // from a different thread which does not have TaskContext set; see SPARK-18406 for
          // discussion.
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskAttemptId)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          handleLocalReadFailure(blockId)
        }
    }
  }

  /**
    * Get block from the local block manager as serialized bytes.
    */
  def getLocalBytes(blockId: BlockId): Option[BlockData] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      val buf = new ChunkedByteBuffer(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
      Some(new ByteBufferBlockData(buf, true))
    } else {
      blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
    }
  }

  /**
    * Get block from the local block manager as serialized bytes.
    *
    * Must be called while holding a read lock on the block.
    * Releases the read lock upon exception; keeps the read lock upon successful return.
    */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) {
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // The block was not found on disk, so serialize an in-memory copy:
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized
      if (level.useMemory && memoryStore.contains(blockId)) {
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
  }

  /**
    * Get block from remote block managers.
    *
    * This does not acquire a lock on this block in this JVM.
    */
  private def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val ct = implicitly[ClassTag[T]]
    getRemoteBytes(blockId).map { data =>
      val values =
        serializerManager.dataDeserializeStream(blockId, data.toInputStream(dispose = true))(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    }
  }

  /**
    * Return a list of locations for the given block, prioritizing the local machine since
    * multiple block managers can share the same host, followed by hosts on the same rack.
    */
  private def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] = {
    val locs = Random.shuffle(locations)
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    blockManagerId.topologyInfo match {
      case None => preferredLocs ++ otherLocs
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        preferredLocs ++ sameRackLocs ++ differentRackLocs
    }
  }

  /**
    * Independent decoupling part for data shuffles over network
    */
  def fetchBlocks[T: ClassTag](taskId: Long, totalPartitions: Int, parentMap: HashMap[Int, Int], reduceId: Int): Boolean = {

    val startFetch = System.currentTimeMillis
    val outputTracker = mapOutputTracker.asInstanceOf[MapOutputTrackerWorker]
    var realFetched: Boolean = false
    // register the fetch
    outputTracker.startFetching(taskId, totalPartitions)

    val managerToBlocks = new HashMap[BlockManagerId, ArrayBuffer[String]]
    val poisonBlock = ShuffleBlockId(poisonValue, poisonValue, poisonValue)
    var sumShuffleSize = 0L
    var sumShuffleCnt = 0

    parentMap.map { case (shuffleId, parentPartitions) =>

      for (i <- 0 until parentPartitions) {
        var fetchTaken: Boolean = true
        var blockId = ShuffleBlockId(shuffleId, i, reduceId)
        val mapStatus = outputTracker.getLocationsAndStatus(blockId)

        // TODO: fix the mismatch between the blockID and taskID
        // Fetch data from other workers if the input is not co-located
        if (mapStatus.location.host != blockManagerId.host) {
          val blockPart = mapStatus.getId(reduceId)

          val blockSize = (
            try{
              mapStatus.getSizeForBlock(blockPart)
            } catch {
              case ex: ArrayIndexOutOfBoundsException => {
                fetchTaken = false
                0
              }
            }
          )

          if (fetchTaken && blockSize > decouplingThreshold) {
            bytesRemoteRead += blockSize.toLong
            blocksRemoteRead += 1
            sumShuffleSize += blockSize.toLong
            sumShuffleCnt += 1

            if (!managerToBlocks.contains(mapStatus.location)) {
              managerToBlocks.put(mapStatus.location, ArrayBuffer[String]())
            }

            managerToBlocks.getOrElse(mapStatus.location, null) += blockId.toString
          } else {
            outputTracker.finishFetching(taskId, poisonBlock)
          }
        } else {
          outputTracker.finishFetching(taskId, poisonBlock)
        }
      }
    }

    val fetchPrepare = System.currentTimeMillis - startFetch
    //logInfo(s"====To allocate fetch takes ${fetchPrepare} ms")

    if (sumShuffleSize < DECOUPLING_THRESHOLD) {
      // Terminate the decoupling by feeding poison blocks
      for (i <- 0 until sumShuffleCnt) {
        outputTracker.finishFetching(taskId, poisonBlock)
      }
    } else {
      realFetched = true
  
      val startTime = System.currentTimeMillis
      val listener = new BlockFetchingListener {
          override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
            // logInfo(s"====ERROR: onBlockFetchFailure for block ${blockId} due to ${exception}")
            // Handle the error by appending a poison block to terminate decoupling
            mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].finishFetching(taskId, poisonBlock)
          }

          override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
            val fetchDuration = System.currentTimeMillis - startTime
            // logInfo(s"====Done: onBlockFetchSuccess for block ${blockId}, task ${taskId} took ${fetchDuration} ms")
            val tLevel = StorageLevel.MEMORY_AND_DISK_SER
            val block = BlockId.apply(blockId)
            putBytesShuffle(block, new ChunkedByteBuffer(data.nioByteBuffer()), tLevel, false, true)(implicitly[ClassTag[T]])
            
            outputTracker.finishFetching(taskId, block)
          }
      }

      Future {
        managerToBlocks.map{ case (manager, blocks) => 
                blockTransferService.fetchGaintBlocks(manager.host, manager.port, manager.executorId, blocks.toArray, listener, null)}
      }(futureCommunicationTaskPool)
    }
    // logInfo(s"====bytesRemoteRead is ${bytesRemoteRead}, blocksRemoteRead is ${blocksRemoteRead}")
    
    realFetched
  }

  /**
    * Get block from remote block managers as serialized bytes.
    */
  def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    var runningFailureCount = 0
    var totalFailureCount = 0

    // Because all the remote blocks are registered in driver, it is not necessary to ask
    // all the slave executors to get block status.
    if (!blockId.isBroadcast || !HTTP_BD) {
      val locationsAndStatus = master.getLocationsAndStatus(blockId)
      val blockSize = locationsAndStatus.map { b =>
        b.status.diskSize.max(b.status.memSize)
      }.getOrElse(0L)
      val blockLocations = locationsAndStatus.map(_.locations).getOrElse(Seq.empty)

      // If the block size is above the threshold, we should pass our FileManger to
      // BlockTransferService, which will leverage it to spill the block; if not, then passed-in
      // null value means the block will be persisted in memory.
      val tempFileManager = if (blockSize > maxRemoteBlockToMem) {
        remoteBlockTempFileManager
      } else {
        null
      }

      val locations = sortLocations(blockLocations)
      val maxFetchFailures = locations.size
      var locationIterator = locations.iterator
      while (locationIterator.hasNext) {
        val loc = locationIterator.next()
        logDebug(s"Getting remote block $blockId from $loc")
        val data = try {
          blockTransferService.fetchBlockSync(
            loc.host, loc.port, loc.executorId, blockId.toString, tempFileManager).nioByteBuffer()
        } catch {
          case NonFatal(e) =>
            runningFailureCount += 1
            totalFailureCount += 1

            if (totalFailureCount >= maxFetchFailures) {
              // Give up trying anymore locations. Either we've tried all of the original locations,
              // or we've refreshed the list of locations from the master, and have still
              // hit failures after trying locations from the refreshed list.
              logWarning(s"Failed to fetch block after $totalFailureCount fetch failures. " +
                s"Most recent failure cause:", e)
              return None
            }

            logWarning(s"Failed to fetch remote block $blockId " +
              s"from $loc (failed attempt $runningFailureCount)", e)

            // If there is a large number of executors then locations list can contain a
            // large number of stale entries causing a large number of retries that may
            // take a significant amount of time. To get rid of these stale entries
            // we refresh the block locations after a certain number of fetch failures
            if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
              locationIterator = sortLocations(master.getLocations(blockId)).iterator
              logDebug(s"Refreshed locations from the driver " +
                s"after ${runningFailureCount} fetch failures.")
              runningFailureCount = 0
            }

            // This location failed, so we retry fetch from a different one by returning null here
            null
        }

        if (data != null) {
          return Some(new ChunkedByteBuffer(data))
        }
        logDebug(s"The value of block $blockId is null")
      }
    } else {
      val data = master.getBlock(blockId).nioByteBuffer()
      if (data != null) {
        // logInfo(s"=====Successfully get block ${blockId.toString} from master")
        return Some(new ChunkedByteBuffer(data))
      }
    }

    // logInfo(s"====Error: Block $blockId not found")
    None
  }

  /**
    * Get a block from the block manager (either local or remote).
    *
    * This acquires a read lock on the block if the block was stored locally and does not acquire
    * any locks if the block was fetched from a remote block manager. The read lock will
    * automatically be freed once the result's `data` iterator is fully consumed.
    */
  def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      // logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      // logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
    * Downgrades an exclusive write lock to a shared read lock.
    */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
    * Release a lock on the given block with explicit TID.
    * The param `taskAttemptId` should be passed in case we can't get the correct TID from
    * TaskContext, for example, the input iterator of a cached RDD iterates to the end in a child
    * thread.
    */
  def releaseLock(blockId: BlockId, taskAttemptId: Option[Long] = None): Unit = {
    blockInfoManager.unlock(blockId, taskAttemptId)
  }

  /**
    * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
    */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
    * Release all locks for the given task.
    *
    * @return the blocks whose locks were released.
    */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
    * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
    * to compute the block, persist it, and return its values.
    *
    * @return either a BlockResult if the block was successfully cached, or an iterator if the block
    *         could not be cached.
    */
  def getOrElseUpdate[T](
                          blockId: BlockId,
                          level: StorageLevel,
                          classTag: ClassTag[T],
                          makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
      // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
        Right(iter)
    }
  }

  /**
    * @return true if the block was stored or false if an error occurred.
    */
  def putIterator[T: ClassTag](
                                blockId: BlockId,
                                values: Iterator[T],
                                level: StorageLevel,
                                tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster) match {
      case None =>
        true
      case Some(iter) =>
        // Caller doesn't care about the iterator values, so we can close the iterator here
        // to free resources earlier
        iter.close()
        false
    }
  }

  /**
    * A short circuited method to get a block writer that can write data directly to disk.
    * The Block will be appended to the File specified by filename. Callers should handle error
    * cases.
    */
  def getDiskWriter(
                     blockId: BlockId,
                     file: File,
                     serializerInstance: SerializerInstance,
                     bufferSize: Int,
                     writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }

  /**
    * Put a new block of serialized bytes to the block manager.
    *
    * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
    * so may corrupt or change the data stored by the `BlockManager`.
    *
    * @return true if the block was stored or false if an error occurred.
    */
  def putBytesShuffle[T: ClassTag](
                             blockId: BlockId,
                             bytes: ChunkedByteBuffer,
                             level: StorageLevel,
                             tellMaster: Boolean = true,
                             isShuffleBlock: Boolean = false): Boolean = {

    doPut(blockId, level, implicitly[ClassTag[T]], tellMaster = false, keepReadLock = false) { info =>
      val startTimeMs = System.currentTimeMillis
      val size = bytes.size
      var putSucceeded: Boolean = false 

      fetchedBlocksTillNow.containsKey(blockId.name) match {
        case true => fetchedBlocksTillNow.put(blockId.name, fetchedBlocksTillNow.get(blockId.name) + 1)
        case _ => fetchedBlocksTillNow.put(blockId.name, 1)
      }

      if (IN_MEMORY_SHUFFLE) {
        putSucceeded = {
          val memoryMode = level.memoryMode
          memoryStore.putBytes(blockId, size, memoryMode, () => {
            if (memoryMode == MemoryMode.OFF_HEAP &&
              bytes.chunks.exists(buffer => !buffer.isDirect)) {
              bytes.copy(Platform.allocateDirectBuffer)
            } else {
              bytes
            }
          })
        }
      }

      if (!putSucceeded && level.useDisk) {
        logInfo(s"Persisting block $blockId to disk instead.")
        diskStore.putBytes(blockId, bytes)
      }

      logInfo("Put block %s locally took %s, size is %s".format(blockId, Utils.getUsedTimeMs(startTimeMs), size))
      
      None
    }.isEmpty
  }

  /**
    * Put a new block of serialized bytes to the block manager.
    *
    * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
    * so may corrupt or change the data stored by the `BlockManager`.
    *
    * @return true if the block was stored or false if an error occurred.
    */
  def putBytes[T: ClassTag](
                             blockId: BlockId,
                             bytes: ChunkedByteBuffer,
                             level: StorageLevel,
                             tellMaster: Boolean = true,
                             isShuffleBlock: Boolean = false): Boolean = {
    if (!isShuffleBlock)
      require(bytes != null, "Bytes is null")

    doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster, false, isShuffleBlock)
  }

  /**
    * Put the given bytes according to the given level in one of the block stores, replicating
    * the values if necessary.
    *
    * If the block already exists, this method will not overwrite it.
    *
    * '''Important!''' Callers must not mutate or release the data buffer underlying `bytes`. Doing
    * so may corrupt or change the data stored by the `BlockManager`.
    *
    * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
    *                     block already exists). If false, this method will hold no locks when it
    *                     returns.
    * @return true if the block was already present or if the put succeeded, false otherwise.
    */
  private def doPutBytes[T](
                             blockId: BlockId,
                             bytes: ChunkedByteBuffer,
                             level: StorageLevel,
                             classTag: ClassTag[T],
                             tellMaster: Boolean = true,
                             keepReadLock: Boolean = false,
                             isShuffleBlock: Boolean = false): Boolean = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      // Since we're storing bytes, initiate the replication before storing them locally.
      // This is faster as data is already serialized and ready to send.
      //logInfo(s"====Check whether to replicate with ${level.replication}")
      val replicationFuture = if (level.replication > 1) {
        if (!ASYN_MASTER){
          Future {
            // This is a blocking action and should run in futureExecutionContext which is a cached
            // thread pool. The ByteBufferBlockData wrapper is not disposed of to avoid releasing
            // buffers that are owned by the caller.
            //logInfo(s"====Try to replicate now ${level.replication}")
            replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
          }(futureExecutionContext)
        } else {
          replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
          null
        }
      } else {
        null
      }

      val size = bytes.size

      if (level.useMemory && !isShuffleBlock) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        val putSucceeded = if (level.deserialized) {
          val values =
            serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
          memoryStore.putIteratorAsValues(blockId, values, classTag) match {
            case Right(_) => true
            case Left(iter) =>
              // If putting deserialized values in memory failed, we will put the bytes directly to
              // disk, so we don't need this iterator and can close it to free resources earlier.
              iter.close()
              false
          }
        } else {
          val memoryMode = level.memoryMode
          memoryStore.putBytes(blockId, size, memoryMode, () => {
            if (memoryMode == MemoryMode.OFF_HEAP &&
              bytes.chunks.exists(buffer => !buffer.isDirect)) {
              bytes.copy(Platform.allocateDirectBuffer)
            } else {
              bytes
            }
          })
        }
        if (!putSucceeded && level.useDisk) {
          logInfo(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
      } else if (level.useDisk) {
        diskStore.putBytes(blockId, bytes)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      val tellStart = System.currentTimeMillis
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store,
        // tell the master about it.
        info.size = size
        if (ASYN_MASTER) {
          Future {
            if (tellMaster && info.tellMaster) {
              reportBlockStatus(blockId, putBlockStatus)
            }
          } (futureExecutionContext)
        } else {
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
        }

        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
      }
      logInfo("Put block %s locally took %s, tell master took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs), Utils.getUsedTimeMs(tellStart)))
      if (level.replication > 1 && !ASYN_MASTER) {
        // Wait for asynchronous replication to finish
        try {
          ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
        } catch {
          case NonFatal(t) =>
            throw new Exception("Error occurred while waiting for replication to finish", t)
        }
      }
      if (blockWasSuccessfullyStored) {
        None
      } else {
        Some(bytes)
      }
    }.isEmpty
  }

  /**
    * Helper method used to abstract common code from [[doPutBytes()]] and [[doPutIterator()]].
    *
    * @param putBody a function which attempts the actual put() and returns None on success
    *                or Some on failure.
    */
  private def doPut[T](
                        blockId: BlockId,
                        level: StorageLevel,
                        classTag: ClassTag[_],
                        tellMaster: Boolean,
                        keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeMs = System.currentTimeMillis
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } catch {
      // Since removeBlockInternal may throw exception,
      // we should print exception first to show root cause.
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      // This cleanup is performed in a finally block rather than a `catch` to avoid having to
      // catch and properly re-throw InterruptedException.
      if (exceptionWasThrown) {
        // If an exception was thrown then it's possible that the code in `putBody` has already
        // notified the master about the availability of this block, so we need to send an update
        // to remove this block location.
        removeBlockInternal(blockId, tellMaster = tellMaster)
        // The `putBody` code may have also added a new block status to TaskMetrics, so we need
        // to cancel that out by overwriting it with an empty block status. We only do this if
        // the finally block was entered via an exception because doing this unconditionally would
        // cause us to send empty block statuses for every block that failed to be cached due to
        // a memory shortage (which is an expected failure, unlike an uncaught exception).
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    if (level.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }
    result
  }

  /**
    * Put the given block according to the given level in one of the block stores, replicating
    * the values if necessary.
    *
    * If the block already exists, this method will not overwrite it.
    *
    * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
    *                     block already exists). If false, this method will hold no locks when it
    *                     returns.
    * @return None if the block was already present or if the put succeeded, or Some(iterator)
    *         if the put failed.
    */
  private def doPutIterator[T](
                                blockId: BlockId,
                                iterator: () => Iterator[T],
                                level: StorageLevel,
                                classTag: ClassTag[T],
                                tellMaster: Boolean = true,
                                keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]] = {
    doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info =>
      val startTimeMs = System.currentTimeMillis
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else { // !level.deserialized
          memoryStore.putIteratorAsBytes(blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              // Not enough space to unroll this block; drop to disk if applicable
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(partiallySerializedValues.valuesIterator)
              }
          }
        }

      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory or disk store, tell the master about it.
        info.size = size
        val tellStart = System.currentTimeMillis
        Future {
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
        } (futureExecutionContext)
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)

        logInfo("Put block %s locally took %s, tell master took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs), Utils.getUsedTimeMs(tellStart)))
        if (level.replication > 1) {
          val remoteStartTime = System.currentTimeMillis
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          // [SPARK-16550] Erase the typed classTag when using default serialization, since
          // NettyBlockRpcServer crashes when deserializing repl-defined classes.
          // TODO(ekl) remove this once the classloader issue on the remote end is fixed.
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logInfo("Replicate block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
  }

  /**
    * Attempts to cache spilled bytes read from disk into the MemoryStore in order to speed up
    * subsequent reads. This method requires the caller to hold a read lock on the block.
    *
    * @return a copy of the bytes from the memory store if the put succeeded, otherwise None.
    *         If this returns bytes from the memory store then the original disk store bytes will
    *         automatically be disposed and the caller should not continue to use them. Otherwise,
    *         if this returns None then the original disk store bytes will be unaffected.
    */
  private def maybeCacheDiskBytesInMemory(
                                           blockInfo: BlockInfo,
                                           blockId: BlockId,
                                           level: StorageLevel,
                                           diskData: BlockData): Option[ChunkedByteBuffer] = {
    require(!level.deserialized)
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(blockId, diskData.size, level.memoryMode, () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we
            // cannot put it into MemoryStore, copyForMemory should not be created. That's why
            // this action is put into a `() => ChunkedByteBuffer` and created lazily.
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
    * Attempts to cache spilled values read from disk into the MemoryStore in order to speed up
    * subsequent reads. This method requires the caller to hold a read lock on the block.
    *
    * @return a copy of the iterator. The original iterator passed this method should no longer
    *         be used after this method returns.
    */
  private def maybeCacheDiskValuesInMemory[T](
                                               blockInfo: BlockInfo,
                                               blockId: BlockId,
                                               level: StorageLevel,
                                               diskIterator: Iterator[T]): Iterator[T] = {
    require(level.deserialized)
    val classTag = blockInfo.classTag.asInstanceOf[ClassTag[T]]
    if (level.useMemory) {
      // Synchronize on blockInfo to guard against a race condition where two readers both try to
      // put values read from disk into the MemoryStore.
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          // Note: if we had a means to discard the disk iterator, we would do that here.
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              // The memory store put() failed, so it returned the iterator back to us:
              iter
            case Right(_) =>
              // The put() succeeded, so we can read the values back:
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
  }

  /**
    * Get peer block managers in the system.
    */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      var cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      if (isLocal && ASYN_MASTER) {
        cachedPeersTtl = 10 * 1000
      }
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      val startGet = System.currentTimeMillis
      if (cachedPeers == null || forceFetch || timeout || cachedPeers.size == 0) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        val getTime = System.currentTimeMillis - startGet
        logInfo(s"Fetched peers from master took ${getTime} ms: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
    * Called for pro-active replenishment of blocks lost due to executor failures
    *
    * @param blockId blockId being replicate
    * @param existingReplicas existing block managers that have a replica
    * @param maxReplicas maximum replicas needed
    */
  def replicateBlock(
                      blockId: BlockId,
                      existingReplicas: Set[BlockManagerId],
                      maxReplicas: Int): Unit = {
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    blockInfoManager.lockForReading(blockId).foreach { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      // we know we are called as a result of an executor removal, so we refresh peer cache
      // this way, we won't try to replicate to a missing executor with a stale reference

      getPeers(forceFetch = true)
      try {
        replicate(blockId, data, storageLevel, info.classTag, existingReplicas)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
  }

  // first broadcast the mapStatus, and then uploadBlocks
  def broadcastBlockData(blockId: BlockId, mapStatus: MapStatus, numPartitions: Int): Unit = {

    //logInfo(s"====Start to broadcastBlockData for blockId ${blockId.name}")
    val shuffleBlockManager = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val shuffleId = blockId.asInstanceOf[ShuffleBlockId].shuffleId
    val mapId = blockId.asInstanceOf[ShuffleBlockId].mapId

    // broadcast the mapStatus, given a NULL data
    val data = shuffleBlockManager.getBlockIndexNew(ShuffleIndexBlockId(shuffleId, mapId, 0), false)
    broadcastMapStatus(TestBlockId("0"), data, mapStatus, System.currentTimeMillis.toString + "@" + ShuffleBlockId(shuffleId, mapId, numPartitions).toString + "@" + executorId)

    logInfo(s"====For blockId: ${blockId.name}, getSize is ${mapStatus.getSize}")

    // broadcast pieces of data
    if (mapStatus.getSize > 0) {
      Future{
        val blockDatas = new HashMap[BlockId, ManagedBuffer]

        var inReading: Boolean = true
        var part: Int = 0

        while (inReading) {
          try {
            val partSize = mapStatus.getSizeForBlock(part)
            if (partSize > 0 && partSize <= BROADCAST_THRESHOLD) {
              val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, part)
              blockDatas.put(shuffleBlockId, shuffleBlockManager.getBlockData(shuffleBlockId))
            }
            part = part + 1
          } catch {
            case ex: ArrayIndexOutOfBoundsException => inReading = false
          }
        }

        if (blockDatas.nonEmpty) {
          anycastBatchBlocks(blockDatas)
        }
      } (futureExecutionContext)
    }
  }

  override def handleUploadBatchBlocksAsyn(uploadBatchBlocks: UploadBatchBlocks, serializer: Serializer): Unit = {
    val blockDatas = uploadBatchBlocks.blockData
    val blockIds = uploadBatchBlocks.blockId
    val startWait = System.currentTimeMillis
    logInfo(s"====handleUploadBatchBlocksAsyn: blockDatas size ${blockDatas.size}, blockIds size ${blockIds.size}")
    // direct data file

    if (!blockIds(0).contains(".")) {
      Future {
        val startTime = System.currentTimeMillis
        val (info: String) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBatchBlocks.metadata))
            .asInstanceOf[(String)]
        }

        var index: Int = 0
        var totalSize: Long = 0
        blockDatas.map { case blockData =>
          // convert from string to bytebuffer
          val data = new NioManagedBuffer(ByteBuffer.wrap(blockData))
          putShuffleBlockDirectData(BlockId.apply(blockIds(index)), data, info)

          index = index + 1
          totalSize += data.size
        }

        val duration = System.currentTimeMillis - startTime
        val waitTime = startTime - startWait
        logInfo(s"====uploadBatchBlocks have size ${blockDatas.size}, TotalSize ${totalSize}, info: ${info}, takes ${duration} ms, wait ${waitTime} ms")
      } (futureExecutionContext)
    } else {
        val (mapStatus: MapStatus, info: String) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBatchBlocks.metadata))
            .asInstanceOf[(MapStatus, String)]
        }

        var index: Int = 0
        blockDatas.map { case blockData => 
            val data = new NioManagedBuffer(ByteBuffer.wrap(blockData))
            putShuffleBlockData(BlockId.apply(blockIds(index)), data, mapStatus, info)

            index += 1
        }
    }
  }

  def tryToRemoveBlock(blockId: BlockId): Unit = {
    val blockName = blockId.name

    fetchedBlocksTillNow.containsKey(blockName) match {
      case true =>
        val currentCount = fetchedBlocksTillNow.get(blockName) 
        fetchedBlocksTillNow.put(blockName, currentCount - 1)

        if (currentCount == 1) {
          mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].fetchRemove(blockName)
          fetchedBlocksTillNow.remove(blockName)
          removeBlockInternal(blockId, false)
        }
      case _ => 
        logInfo(s"====Error: Impossible to remove block ${blockName}")
    }
  }

  def addChildPlacement(parentInfo: String, reduceId: Int, placementId: String): Unit = {
    // check whether some segments on this host
    logInfo(s"====addChildPlacement for parentInfo ${parentInfo}, reduceID ${reduceId}, placementId ${placementId}")
    
    val parents = parentInfo.split("_")

    shuffleLock.lock()
    parents.map {
      case parent =>
        // put the request to pending queue
        // first, get the lock to add item for this shuffle 
        if (!shufflePendingRecord.containsKey(parent))
          shufflePendingRecord.put(parent, new HashMap[Int, String])
        val placementIds = shufflePendingRecord.get(parent).getOrElse(reduceId, " ")

        shufflePendingRecord.get(parent).put(reduceId, placementIds + "_" + placementId)
        // [non-blocking] Push the completed maps
        if (!CENTRAL_TEST_SUITE)
          pushOutputResponse(parent, reduceId, placementId)
    }
    shuffleLock.unlock()
  }

  def pushOutputResponse(_shuffleId: String, reduceID: Int, placementId: String): Unit = {
    // get the completed maps
    val shuffleId = _shuffleId.toInt
    val outputTracker = mapOutputTracker.asInstanceOf[MapOutputTrackerWorker]
    val startTime = System.currentTimeMillis
    
    var count: Int = 1
    var unregister: Int = 0


    outputTracker.completedMapPartitions.synchronized {
      if (outputTracker.completedMapPartitions.containsKey(_shuffleId)) {

        val completedMaps = outputTracker.completedMapPartitions.get(_shuffleId).toArray
        val shuffleStatus = outputTracker.shuffleStatuses.get(shuffleId)
        val shuffleBlockManager = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]

        logInfo(s"====pushOutputResponse for _shuffleId ${_shuffleId}, reduceID ${reduceID}, placementId ${placementId}, completedPartitions ${completedMaps.size}")

        var blockDatas = new HashMap[String, HashMap[BlockId, ManagedBuffer]]
        blockDatas.put(placementId, new HashMap[BlockId, ManagedBuffer]())

        completedMaps.map {
          case mapId =>
            val mapstatus = shuffleStatus.mapStatuses(mapId)
            val reduceId = mapstatus.getId(reduceID)
            val partitionSize = mapstatus.getSizeForBlock(reduceId)

            if (reduceId != reduceID)
              logInfo(s"====Warning: reduceId ${reduceId} != reduceID ${reduceID} for shuffleId ${_shuffleId}, mapId ${mapId}, placementId ${placementId}")

            if (partitionSize > 0) {
              // proactively push 
              if (partitionSize < PUSH_THRESHOLD) {
                if (count % BATCH_SIZE == 0) {
                  pushBatchBlocks(blockDatas)
                  blockDatas = new HashMap[String, HashMap[BlockId, ManagedBuffer]]
                  blockDatas.put(placementId, new HashMap[BlockId, ManagedBuffer]())
                }

                val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
                val blockData = shuffleBlockManager.getBlockData(shuffleBlockId)
                blockDatas.get(placementId).orNull.put(shuffleBlockId, blockData)
                logInfo(s"====pushOutputResponse:pushBatchBlocks blocks ${shuffleBlockId.toString}, to ${placementId}")

                count += 1
              } else {
                unregister += 1
              }
            }
            //logInfo(s"====pushOutputResponse to ${placementId}, for shuffleId ${shuffleId}, mapId ${mapId}, reduceId ${reduceId}, size ${partitionSize}")
        }

        if (blockDatas.nonEmpty)
          pushBatchBlocks(blockDatas)
      }
      else {
        logInfo(s"====Error: Do not get shuffle for ${_shuffleId}, reduceID ${reduceID}")
      }
    }
    
    val duration = System.currentTimeMillis - startTime
    count -= 1
    logInfo(s"====addChildPlacement takes ${duration} ms, register ${count}, unregister ${unregister}")
  }

  def pushOutputRequest(_shuffleId: String, mapId: Int): Unit = {

    if (CENTRAL_TEST_SUITE) {
      centralTestSuite(_shuffleId, mapId)
    } else {
      val startTime = System.currentTimeMillis
      // load the child placements
      val shuffleId = _shuffleId.toInt
      val outputTracker = mapOutputTracker.asInstanceOf[MapOutputTrackerWorker]
      val shufflestatus = outputTracker.shuffleStatuses.get(shuffleId)

      val mapStatus = shufflestatus.mapStatuses(mapId)
      val mapOutputSize = mapStatus.getSize
      val numPartitions = shufflestatus.getPartitions()

      var register: Int = 0
      var unregister: Int = 0

      // [executorId, [shuffleBlockId, BlockData]]
      val blockDatas = new HashMap[String, HashMap[BlockId, ManagedBuffer]]
      val shuffleBlockManager = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
      val data = shuffleBlockManager.getBlockIndexNew(ShuffleIndexBlockId(shuffleId, mapId, 0), false)

      broadcastMapStatus(TestBlockId("0"), data, mapStatus, System.currentTimeMillis.toString + "@" + ShuffleBlockId(shuffleId, mapId, numPartitions).toString  + "@" + executorId)
      
      val startLockTime = System.currentTimeMillis
      var doneLockTime = 0L

      // if no push request is running, then will be the first one to get the lock
      shufflePendingRequest.synchronized {
        val curReq = shufflePendingRequest.incrementAndGet()
        if (curReq == 1) {
          shuffleLock.lock()
        }
      }


      doneLockTime = System.currentTimeMillis - startLockTime
      val _accumulateGetData = System.currentTimeMillis 

      if (shufflePendingRecord.containsKey(_shuffleId)) {
        // stop write in here 
        val reduceIDMaps = shufflePendingRecord.get(_shuffleId)

        reduceIDMaps.map{
          case (reduceID, executorId) => 
            val reduceId = mapStatus.getId(reduceID)
            val partitionSize = mapStatus.getSizeForBlock(reduceId)

            logInfo(s"====reduceIDMaps for shuffleId ${shuffleId}, mapId ${mapId}, reduceID ${reduceID}, reduceId ${reduceId}, executorId ${executorId}, size is ${partitionSize}")
            if (partitionSize > 0) {
              // proactively push 
              if (partitionSize < PUSH_THRESHOLD) {
                val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
                val blockData = shuffleBlockManager.getBlockData(shuffleBlockId)
 
                val executorIds = executorId.split("_")
                executorIds.slice(1, executorIds.size).map {
                  case exe => 
                    if (!blockDatas.contains(exe)) {
                      blockDatas.put(exe, new HashMap[BlockId, ManagedBuffer]())
                    }
                    blockDatas.get(exe).orNull.put(shuffleBlockId, blockData)
                    //pushBatchBlocks(x, Array(shuffleBlockId.toString), Array(blockData))
                }
                register += 1 
              } else {
                unregister += 1
              }
            }
        }
      }

      val addPartitionStart = System.currentTimeMillis
      val accumulateGetData = addPartitionStart - _accumulateGetData

      outputTracker.addcompletedMapPartitions(_shuffleId, mapId)
      
      // release the lock if necessary
      shufflePendingRequest.synchronized {
        val remainingReq = shufflePendingRequest.decrementAndGet()

        if (remainingReq == 0) {
          shuffleLock.unlock()
        }
      }

      // start to send batch blocks
      if (blockDatas.nonEmpty) {
        Future {
          pushBatchBlocks(blockDatas)
        } (futureExecutionContext)
      }

      val fromPartition = System.currentTimeMillis - addPartitionStart
      val duration = System.currentTimeMillis - startTime

      logInfo(s"====pushOutputRequest takes ${duration} ms, for ${shuffleId}, with mapId ${mapId}, ispending ${shufflePendingRecord.containsKey(_shuffleId)}, push ${blockDatas.size}, register ${register}, unregister ${unregister}, getLock takes ${doneLockTime} ms, accumulateGetData ${accumulateGetData}, partition ${fromPartition} ms")
    
    }
  }

  
  // test for the centralized GDA
  def centralTestSuite(_shuffleId: String, mapId: Int): Unit = {

    val startTime = System.currentTimeMillis
    // load the child placements
    val shuffleId = _shuffleId.toInt
    val outputTracker = mapOutputTracker.asInstanceOf[MapOutputTrackerWorker]
    val shufflestatus = outputTracker.shuffleStatuses.get(shuffleId)

    val mapStatus = shufflestatus.mapStatuses(mapId)
    val mapOutputSize = mapStatus.getSize
    val numPartitions = shufflestatus.getPartitions()

    var register: Int = 0
    var unregister: Int = 0

    // [executorId, [shuffleBlockId, BlockData]]
    val blockDatas = new HashMap[String, HashMap[BlockId, ManagedBuffer]]
    val shuffleBlockManager = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val data = shuffleBlockManager.getBlockIndexNew(ShuffleIndexBlockId(shuffleId, mapId, 0), false)

    broadcastMapStatus(TestBlockId("0"), data, mapStatus, System.currentTimeMillis.toString + "@" + ShuffleBlockId(shuffleId, mapId, numPartitions).toString  + "@" + executorId)
    
    //logInfo(s"====shufflePendingRecord.containsKey check ${shuffleId}")

    if (blockManagerId.host != "100.0.0.1") {
      var centralExecutorId = "-1"

      for (peer <- blockTransferService.asInstanceOf[NettyBlockTransferService].peersAddress) {
        val peerHost = peer.split(':')(0).replace("/","")
        if (peerHost == "100.0.0.1") {
          centralExecutorId = peer.split('#')(1)
          //logInfo(s"====Successfully find centralized host with executor ${centralExecutorId}")
        }
      }

      if (centralExecutorId == "-1") {
        logInfo(s"====Failed to find centralized host ")
      }

      for (reduceID <- 0 until numPartitions) {
        val reduceId = mapStatus.getId(reduceID)
        val partitionSize = mapStatus.getSizeForBlock(reduceId)

        if (partitionSize > 0) {
          // proactively push 
          if (partitionSize < PUSH_THRESHOLD) {
            val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
            val blockData = shuffleBlockManager.getBlockData(shuffleBlockId)

            if (!blockDatas.contains(centralExecutorId)) {
              blockDatas.put(centralExecutorId, new HashMap[BlockId, ManagedBuffer]())
            }
            blockDatas.get(centralExecutorId).orNull.put(shuffleBlockId, blockData)
            
            logInfo(s"====pushOutputRequest:pushBatchBlocks blocks ${shuffleBlockId.toString}, to ${centralExecutorId}, with mapId ${mapId}, reduceID ${reduceID}, reduceId ${reduceId}")
            //val executorIds = executorId.split("_")
            //pushBatchBlocks(centralExecutorId, Array(shuffleBlockId.toString), Array(blockData))
            register += 1 
          } else {
            unregister += 1
          }
        }

        outputTracker.completedMapPartitions.synchronized {
          outputTracker.addcompletedMapPartitions(_shuffleId, mapId)
        }
      }

      // start to send batch blocks
      if (blockDatas.nonEmpty) {
        pushBatchBlocks(blockDatas)
      }
    }

    val duration = System.currentTimeMillis - startTime
    logInfo(s"====centralTestSuite takes ${duration} ms, for ${shuffleId}, with mapId ${mapId}, ispending ${shufflePendingRecord.containsKey(_shuffleId)}, push ${blockDatas.size}, register ${register}, unregister ${unregister}")
  }

  def broadcastData(indexBlock: BlockId, dataBlock: BlockId, mapStatus: MapStatus, info: String): Unit = {
    var indexData: ManagedBuffer = null
    var fileData: ManagedBuffer = null
    var datas = new HashMap[BlockId, ManagedBuffer]

    if (!info.contains("#")) {
      indexData = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver].getBlockIndexNew(indexBlock.asInstanceOf[ShuffleIndexBlockId])
      fileData = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver].getBlockDataNew(dataBlock.asInstanceOf[ShuffleDataBlockId])

      datas.put(dataBlock, fileData)
      datas.put(indexBlock, indexData)
    } else {
      indexData = shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver].getBlockIndexNew(indexBlock.asInstanceOf[ShuffleIndexBlockId], false)
      datas.put(indexBlock, indexData)
    }

    broadcastBatchBlocks(datas, mapStatus, info)
  }

  /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def anycastBlocks(blockDatas: HashMap[BlockId, ManagedBuffer]): Unit = {
    var info: String = System.currentTimeMillis.toString

    for (peer <- blockTransferService.asInstanceOf[NettyBlockTransferService].peersAddress) {
      val peerHost = peer.split(':')(0).replace("/","")
      val peerPort = peer.split(':')(1).split('#')(0).toInt
      val peerId = peer.split('#')(1)

      blockDatas.map { case (blockId, data) => 
        blockTransferService.asInstanceOf[NettyBlockTransferService].uploadShuffleBlocks(
              peerHost,
              peerPort,
              peerId,
              blockId,
              data,
              (info + "@" + blockManagerId.executorId)
        )

        logInfo(s"====Try to anycast block: ${blockId.name}")
      }
    }
  }


  /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def broadcastBatchBlocks(blockDatas: HashMap[BlockId, ManagedBuffer], mapStatus: MapStatus, info: String): Unit = {
    var blockIds = new Array[String](blockDatas.size)
    var datas = new Array[ManagedBuffer](blockDatas.size)
    var index: Int = 0

    blockDatas.map { case (blockId, data) => 
      blockIds(index) = blockId.toString
      datas(index) = data
      index = index + 1
    }

    for (peer <- blockTransferService.asInstanceOf[NettyBlockTransferService].peersAddress) {
      val peerHost = peer.split(':')(0).replace("/","")
      val peerPort = peer.split(':')(1).split('#')(0).toInt
      val peerId = peer.split('#')(1)

      blockTransferService.asInstanceOf[NettyBlockTransferService].uploadBatchBlocks(
        peerHost,
        peerPort,
        peerId,
        blockIds,
        datas,
        info,
        Some(mapStatus)
      )
    }
  }

 /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def pushBatchBlocks(executorId: String, blockIds: Array[String], datas: Array[ManagedBuffer]): Unit = {
    val startTime = System.currentTimeMillis 
    var info: String = startTime.toString + "@" + blockManagerId.executorId

    while (!blockTransferService.asInstanceOf[NettyBlockTransferService].peersMap.containsKey(executorId)) {
      Thread.sleep(10)
      logInfo(s"====Do not get the peer address now ${executorId}")
    }
    
    val peerAddress = blockTransferService.asInstanceOf[NettyBlockTransferService].peersMap.get(executorId)
    val peerHost = peerAddress.split(':')(0).replace("/","")
    val peerPort = peerAddress.split(':')(1).split('#')(0).toInt

    blockTransferService.asInstanceOf[NettyBlockTransferService].uploadBatchBlocks(
      peerHost,
      peerPort,
      executorId,
      blockIds,
      datas,
      info
    )

    val duration = System.currentTimeMillis - startTime
    logInfo(s"====pushBatchBlocks takes ${duration} ms")
  }

  /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def pushBatchBlocks(blockDatas: HashMap[String, HashMap[BlockId, ManagedBuffer]]): Unit = {
    val startTime = System.currentTimeMillis 
    var info: String = startTime.toString + "@" + blockManagerId.executorId

    blockDatas.map {
      case (executorId, blockMaps) =>
        if (blockMaps.size > 0) {
          var blockIds = new Array[String](blockMaps.size)
          var datas = new Array[ManagedBuffer](blockMaps.size)
          var index: Int = 0
          var peerAddress: String = ""

          blockMaps.map {
            case (blockId, data) => 
              blockIds(index) = blockId.toString
              logInfo(s"====pushBatchBlocks blocks ${blockId.toString}, to ${executorId}")
              datas(index) = data
              index = index + 1
          }

          while (!blockTransferService.asInstanceOf[NettyBlockTransferService].peersMap.containsKey(executorId)) {
            Thread.sleep(10)
            logInfo(s"====Do not get the peer address now ${executorId}")
          }
          
          peerAddress = blockTransferService.asInstanceOf[NettyBlockTransferService].peersMap.get(executorId)

          val peerHost = peerAddress.split(':')(0).replace("/","")
          val peerPort = peerAddress.split(':')(1).split('#')(0).toInt

          blockTransferService.asInstanceOf[NettyBlockTransferService].uploadBatchBlocks(
            peerHost,
            peerPort,
            executorId,
            blockIds,
            datas,
            info
          )
        }
    }

    val duration = System.currentTimeMillis - startTime
    logInfo(s"====pushBatchBlocks takes ${duration} ms")
  }

  /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def anycastBatchBlocks(blockDatas: HashMap[BlockId, ManagedBuffer]): Unit = {
    var info: String = System.currentTimeMillis.toString
    var blockIds = new Array[String](blockDatas.size)
    var datas = new Array[ManagedBuffer](blockDatas.size)
    var index: Int = 0

    blockDatas.map { case (blockId, data) => 
      blockIds(index) = blockId.toString
      datas(index) = data
      index = index + 1
    }

    for (peer <- blockTransferService.asInstanceOf[NettyBlockTransferService].peersAddress) {
      val peerHost = peer.split(':')(0).replace("/","")
      val peerPort = peer.split(':')(1).split('#')(0).toInt
      val peerId = peer.split('#')(1)

      blockTransferService.asInstanceOf[NettyBlockTransferService].uploadBatchBlocks(
        peerHost,
        peerPort,
        peerId,
        blockIds,
        datas,
        (info + "@" + blockManagerId.executorId)
      )
    }
  }

  /**
    * Replicate block to another node. Note that this is a non-blocking call.
    */
  private def broadcastMapStatus(
                         blockId: BlockId,
                         data: ManagedBuffer,
                         mapStatus: MapStatus,
                         info: String
                         ): Unit = {

    for (peer <- blockTransferService.asInstanceOf[NettyBlockTransferService].peersAddress) {
      val peerHost = peer.split(':')(0).replace("/","")
      val peerPort = peer.split(':')(1).split('#')(0).toInt
      val peerId = peer.split('#')(1)
      blockTransferService.asInstanceOf[NettyBlockTransferService].uploadShuffleBlock(
            peerHost,
            peerPort,
            peerId,
            blockId,
            data,
            mapStatus,
            info
      )
    }
  }

  /**
    * Replicate block to another node. Note that this is a blocking call that returns after
    * the block has been replicated.
    */
  private def replicate(
                         blockId: BlockId,
                         data: BlockData,
                         level: StorageLevel,
                         classTag: ClassTag[_],
                         existingReplicas: Set[BlockManagerId] = Set.empty): Unit = {

    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    val bcPeerDegree = conf.getInt("spark.bcPeerOnline.degree", 1)

    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = bcPeerDegree)

    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime

    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0

    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)

    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)

    while(numFailures <= maxReplicationFailures &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logInfo(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        //blockTransferService.uploadBlockSync(

        if (ASYN_MASTER) {
          blockTransferService.uploadBlock(
            peer.host,
            peer.port,
            peer.executorId,
            blockId,
            new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false),
            tLevel,
            classTag)
        } else {
          blockTransferService.uploadBlockSync(
            peer.host,
            peer.port,
            peer.executorId,
            blockId,
            new BlockManagerManagedBuffer(blockInfoManager, blockId, data, false),
            tLevel,
            classTag)
        }
        logInfo(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms, start from ${System.currentTimeMillis}")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          // we have a failed replication, so we get the list of peers again
          // we don't want peers we have already replicated to and the ones that
          // have failed previously
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }

          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }

    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
  }

  /**
    * Read a block consisting of a single object.
    */
  def getSingle[T: ClassTag](blockId: BlockId): Option[T] = {
    get[T](blockId).map(_.data.next().asInstanceOf[T])
  }

  /**
    * Write a block consisting of a single object.
    *
    * @return true if the block was stored or false if the block was already stored or an
    *         error occurred.
    */
  def putSingle[T: ClassTag](
                              blockId: BlockId,
                              value: T,
                              level: StorageLevel,
                              tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
    * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
    * store reaches its limit and needs to free up space.
    *
    * If `data` is not put on disk, it won't be created.
    *
    * The caller of this method must hold a write lock on the block before calling this method.
    * This method does not release the write lock.
    *
    * @return the block's new effective StorageLevel.
    */
  private[storage] override def dropFromMemory[T: ClassTag](
                                                             blockId: BlockId,
                                                             data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // Drop to disk, if storage level requires
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }

    // Actually drop from memory store
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }

    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    status.storageLevel
  }

  /**
    * Remove all blocks belonging to the given RDD.
    *
    * @return The number of blocks removed.
    */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
    * Remove all blocks belonging to the given broadcast.
    */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
    * Remove a block from both memory and disk.
    */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
  }

  /**
    * Internal version of [[removeBlock()]] which assumes that the caller already holds a write
    * lock on the block.
    */
  private def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit = {
    // Removals are idempotent in disk store and memory store. At worst, we get a warning.
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logInfo(s"Block $blockId could not be removed as it was not found on disk or in memory")
    } else {
      logInfo(s"====Have successfully remove block ${blockId.name}")
    }
    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      reportBlockStatus(blockId, BlockStatus.empty)
    }
  }

  private def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit = {
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
  }

  def releaseLockAndDispose(
                             blockId: BlockId,
                             data: BlockData,
                             taskAttemptId: Option[Long] = None): Unit = {
    releaseLock(blockId, taskAttemptId)
    data.dispose()
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo(s"BlockManager stopped, bytesRemoteRead is ${bytesRemoteRead}, blocksRemoteRead is ${blocksRemoteRead}")
  }
}


private[spark] object BlockManager {
  private val ID_GENERATOR = new IdGenerator

  def blockIdsToHosts(
                       blockIds: Array[BlockId],
                       env: SparkEnv,
                       blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }

  private class ShuffleMetricsSource(
                                      override val sourceName: String,
                                      metricSet: MetricSet) extends Source {

    override val metricRegistry = new MetricRegistry
    metricRegistry.registerAll(metricSet)
  }

  class RemoteBlockTempFileManager(blockManager: BlockManager)
    extends TempFileManager with Logging {

    private class ReferenceWithCleanup(file: File, referenceQueue: JReferenceQueue[File])
      extends WeakReference[File](file, referenceQueue) {
      private val filePath = file.getAbsolutePath

      def cleanUp(): Unit = {
        logDebug(s"Clean up file $filePath")

        if (!new File(filePath).delete()) {
          logDebug(s"Fail to delete file $filePath")
        }
      }
    }

    private val referenceQueue = new JReferenceQueue[File]
    private val referenceBuffer = Collections.newSetFromMap[ReferenceWithCleanup](
      new ConcurrentHashMap)

    private val POLL_TIMEOUT = 1000
    @volatile private var stopped = false

    private val cleaningThread = new Thread() { override def run() { keepCleaning() } }
    cleaningThread.setDaemon(true)
    cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
    cleaningThread.start()

    override def createTempFile(): File = {
      blockManager.diskBlockManager.createTempLocalBlock()._2
    }

    override def registerTempFileToClean(file: File): Boolean = {
      referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
    }

    def stop(): Unit = {
      stopped = true
      cleaningThread.interrupt()
      cleaningThread.join()
    }

    private def keepCleaning(): Unit = {
      while (!stopped) {
        try {
          Option(referenceQueue.remove(POLL_TIMEOUT))
            .map(_.asInstanceOf[ReferenceWithCleanup])
            .foreach { ref =>
              referenceBuffer.remove(ref)
              ref.cleanUp()
            }
        } catch {
          case _: InterruptedException =>
          // no-op
          case NonFatal(e) =>
            logError("Error in cleaning thread", e)
        }
      }
    }
  }
}
