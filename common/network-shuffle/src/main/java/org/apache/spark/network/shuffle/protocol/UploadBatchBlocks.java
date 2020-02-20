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

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

public class UploadBatchBlocks extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final String[] blockId;
  // TODO: StorageLevel is serialized separately in here because StorageLevel is not available in
  // this package. We should avoid this hack.
  public final byte[] metadata;
  public final byte[][] blockData;

  public UploadBatchBlocks(String appId, String execId, String[] blockId, byte[] metadata, byte[][] blockData) {
    this.appId = appId;
    this.execId = execId;
    this.blockId = blockId;
    this.metadata = metadata;
    this.blockData = blockData;
  }

  @Override
  protected Type type() { return Type.UPLOAD_BATCH_BLOCKS; }

  @Override
  public int hashCode() {
    int objectsHashCode = Objects.hashCode(appId, execId);
    return (objectsHashCode * 41 + Arrays.hashCode(blockId)) * 41 + Arrays.hashCode(metadata) * 41;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("blockId", Arrays.toString(blockId))
      .add("metadata size", metadata.length)
      .add("block size", blockData.length)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof UploadBatchBlocks) {
      UploadBatchBlocks o = (UploadBatchBlocks) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(execId, o.execId)
        && Arrays.equals(blockId, o.blockId)
        && Arrays.equals(metadata, o.metadata)
        && Arrays.equals(blockData, o.blockData);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    int len = Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Encoders.StringArrays.encodedLength(blockId)
      + Encoders.ByteArrays.encodedLength(metadata)
      + Encoders.Strings.encodedLength(String.valueOf(blockData.length));

    for (int i = 0; i < blockData.length; ++i) {
      len += Encoders.ByteArrays.encodedLength(blockData[i]);
    }

    return len;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    Encoders.StringArrays.encode(buf, blockId);
    Encoders.ByteArrays.encode(buf, metadata);

    Encoders.Strings.encode(buf, String.valueOf(blockData.length));
    for (int i = 0; i < blockData.length; ++i) {
      Encoders.ByteArrays.encode(buf, blockData[i]);
    }
  }

  public static UploadBatchBlocks decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    String[] blockId = Encoders.StringArrays.decode(buf);
    byte[] metadata = Encoders.ByteArrays.decode(buf);
    int _length = Integer.valueOf(Encoders.Strings.decode(buf));

    byte[][] blockData;
    blockData = new byte[_length][];
    for (int i = 0; i < _length; ++i) {
      blockData[i] = Encoders.ByteArrays.decode(buf);
    }

    return new UploadBatchBlocks(appId, execId, blockId, metadata, blockData);
  }
}
