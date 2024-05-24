/*
 * Copyright 2020 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

/**
 * @author Thomas Segismont
 * @author Lukas Prettenthaler
 */
public class IgniteNodeInfo implements Binarylizable {
  private NodeInfo nodeInfo;

  public IgniteNodeInfo() {
  }

  public IgniteNodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo = nodeInfo;
  }

  @Override
  public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
    writer.writeString("host", nodeInfo.host());
    writer.writeInt("port", nodeInfo.port());
    JsonObject metadata = nodeInfo.metadata();
    writer.writeByteArray("meta", metadata != null ? metadata.toBuffer().getBytes() : null);
  }

  @Override
  public void readBinary(BinaryReader reader) throws BinaryObjectException {
    String host = reader.readString("host");
    int port = reader.readInt("port");
    byte[] bytes = reader.readByteArray("meta");
    nodeInfo = new NodeInfo(host, port, bytes != null ? new JsonObject(Buffer.buffer(bytes)) : null);
  }

  public NodeInfo unwrap() {
    return nodeInfo;
  }
}
