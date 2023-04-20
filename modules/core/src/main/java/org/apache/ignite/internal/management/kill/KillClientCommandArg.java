/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.kill;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class KillClientCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(description = "Connection identifier or ALL")
    private String connectionId;

    /** */
    @Argument(description = "Node id to drop connection from", optional = true)
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, connectionId);
        U.writeUuid(out, nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        connectionId = U.readString(in);
        nodeId = U.readUuid(in);
    }

    /** */
    public String getConnectionId() {
        return connectionId;
    }

    /** */
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }

    /** */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }
}
