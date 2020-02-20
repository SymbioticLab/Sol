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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
* Encapsulates a request for a particular chunk of a stream.
*/
public final class StreamChunkId implements Encodable {
  public final long streamId;
  public final int chunkIndex;
  public final int priority;
  public long timestamp;

  public StreamChunkId(long streamId, int chunkIndex, int priority, long timestamp) {
    this.streamId = streamId;
    this.chunkIndex = chunkIndex;
    this.priority = priority;
    this.timestamp = timestamp;
  }

  public StreamChunkId(long streamId, int chunkIndex) {
    this.streamId = streamId;
    this.chunkIndex = chunkIndex;
    this.priority = 1;
    this.timestamp = 0;
  }

  @Override
  public int encodedLength() {
    return 8 + 4 + 4 + 8;
  }

  public void encode(ByteBuf buffer) {
    buffer.writeLong(streamId);
    buffer.writeInt(chunkIndex);
    buffer.writeInt(priority);
    buffer.writeLong(timestamp);
  }

  public static StreamChunkId decode(ByteBuf buffer) {
    assert buffer.readableBytes() >= 8 + 4 + 4 + 8;
    long streamId = buffer.readLong();
    int chunkIndex = buffer.readInt();
    int priority = buffer.readInt();
    long timestamp = buffer.readLong();
    return new StreamChunkId(streamId, chunkIndex, priority, timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, chunkIndex, priority, timestamp);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamChunkId) {
      StreamChunkId o = (StreamChunkId) other;
      return streamId == o.streamId && chunkIndex == o.chunkIndex && priority == o.priority && timestamp == o.timestamp;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("chunkIndex", chunkIndex)
      .add("priority", priority)
      .add("timestamp", timestamp)
      .toString();
  }
}
