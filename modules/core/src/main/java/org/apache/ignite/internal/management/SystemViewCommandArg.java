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

package org.apache.ignite.internal.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.OneOf;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@OneOf(value = {"nodeIds", "nodeId", "allNodes"}, optional = true)
public class SystemViewCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** System view name. */
    @Positional
    @Argument(description = "Name of the system view which content should be printed." +
        " Both \"SQL\" and \"Java\" styles of system view name are supported" +
        " (e.g. SQL_TABLES and sql.tables will be handled similarly)")
    private String systemViewName;

    /** */
    @Argument(
        description = "ID of the node to get the system view from (deprecated. Use --node-ids instead). " +
            "If not set, random node will be chosen",
        optional = true
    )
    private UUID nodeId;

    /** ID of the nodes to get the system view content from. */
    @Argument(
        description = "Comma-separated list of nodes IDs to get the system view from. " +
        "If not set, random node will be chosen",
        example = "nodeId1,nodeId2,..",
        optional = true,
        javaStyleExample = true
    )
    private UUID[] nodeIds;

    /** Flag to get the system view from all nodes. */
    @Argument(
        description = "Get the system view from all nodes. If not set, random node will be chosen",
        optional = true
    )
    private boolean allNodes;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, systemViewName);
        U.writeUuid(out, nodeId);
        U.writeArray(out, nodeIds);
        out.writeBoolean(allNodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        systemViewName = U.readString(in);
        nodeId = U.readUuid(in);
        nodeIds = U.readArray(in, UUID.class);
        allNodes = in.readBoolean();
    }

    /** */
    public String getSystemViewName() {
        return systemViewName;
    }

    /** */
    public void setSystemViewName(String systemViewName) {
        this.systemViewName = systemViewName;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }

    /** */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public UUID[] getNodeIds() {
        return nodeIds;
    }

    /** */
    public void setNodeIds(UUID[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public boolean isAllNodes() {
        return allNodes;
    }

    /** */
    public void setAllNodes(boolean allNodes) {
        this.allNodes = allNodes;
    }
}
