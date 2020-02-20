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

package org.apache.spark.network.netty

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock, UploadBatchBlocks}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.scheduler.MapStatus

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        val startTime = System.currentTimeMillis

        val blocksNum = openBlocks.blockIds.length
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))

        val getBlockDuration = System.currentTimeMillis - startTime
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
        val registerDuration = System.currentTimeMillis - startTime - getBlockDuration

        logInfo(s"Registered streamId $streamId for ${openBlocks.blockIds(0)} with $blocksNum buffers takes $getBlockDuration ms to read, $registerDuration ms to register")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case uploadBatchBlocks: UploadBatchBlocks =>
        val startTime = System.currentTimeMillis
        blockManager.handleUploadBatchBlocksAsyn(uploadBatchBlocks, serializer)
        val duration = System.currentTimeMillis - startTime
        logInfo(s"====Dive into uploadBatchBlocks takes ${duration} ms")
      
      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        // mightbe shuffleBlocks
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)

        if (blockId.name.contains("shuffle")) {
          if (blockId.name.contains(".index") || blockId.name.contains(".data")) {
            // indirect data
            val (mapStatus: MapStatus, info: String) = {
              serializer
                .newInstance()
                .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
                .asInstanceOf[(MapStatus, String)]
            }

            blockManager.putShuffleBlockData(blockId, data, mapStatus, info)
          } else {
            // direct data file
            val (info: String) = {
              serializer
                .newInstance()
                .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
                .asInstanceOf[(String)]
            }

            blockManager.putShuffleBlockDirectData(blockId, data, info)
          }
        } else if (blockId.name.contains("test")) {
          // receive the mapStatus 
          val (mapStatus: MapStatus, info: String) = {
            serializer
              .newInstance()
              .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
              .asInstanceOf[(MapStatus, String)]
          }

          val shuffleBlockId = BlockId.apply(info.split("@")(1))
          val duration = System.currentTimeMillis - info.split("@")(0).toLong
          val executor = info.split("@")(2)
          
          logInfo(s"====Receive testBlock ${shuffleBlockId.toString} takes ${duration} ms from ${executor}")
          blockManager.registerBroadcastMapStatus(shuffleBlockId, mapStatus)
        } else {
          val (level: StorageLevel, classTag: ClassTag[_]) = {
            serializer
              .newInstance()
              .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
              .asInstanceOf[(StorageLevel, ClassTag[_])]
          }
          logInfo(s"====Receive other blocks with ${blockId.toString}")
          blockManager.putBlockData(blockId, data, level, classTag)
          responseContext.onSuccess(ByteBuffer.allocate(0))
        }
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
