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
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@ArgumentGroup(value = {"nodeIds", "nodeId", "allNodes"}, optional = true, onlyOneOf = true)
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
            "If not set, random node will be chosen"
    )
    private UUID nodeId;

    /** ID of the nodes to get the system view content from. */
    @Argument(
        description = "Comma-separated list of nodes IDs to get the system view from. " +
        "If not set, random node will be chosen",
        example = "nodeId1,nodeId2,.."
    )
    private UUID[] nodeIds;

    /** Flag to get the system view from all nodes. */
    @Argument(description = "Get the system view from all nodes. If not set, random node will be chosen")
    private boolean allNodes;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, systemViewName);
        U.writeUuid(out, nodeId);
        U.writeArray(out, nodeIds);
        out.writeBoolean(allNodes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        systemViewName = U.readString(in);
        nodeId = U.readUuid(in);
        nodeIds = U.readArray(in, UUID.class);
        allNodes = in.readBoolean();
    }

    /** */
    public String systemViewName() {
        return systemViewName;
    }

    /** */
    public void systemViewName(String systemViewName) {
        this.systemViewName = systemViewName;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public UUID[] nodeIds() {
        return nodeIds;
    }

    /** */
    public void nodeIds(UUID[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    /** */
    public boolean allNodes() {
        return allNodes;
    }

    /** */
    public void allNodes(boolean allNodes) {
        this.allNodes = allNodes;
    }
}
