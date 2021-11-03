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

package org.apache.ignite.internal.table.distributed.command.scan;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Scan init command for PartitionListener that prepares server-side scan for further iteration over it.
 */
public class ScanInitCommand implements WriteCommand {
    /** Id of the node that requests scan. */
    @NotNull
    private final String requesterNodeId;

    /** Id of scan that is associated with the current command. */
    @NotNull
    private final IgniteUuid scanId;

    /**
     * @param requesterNodeId Id of the node that requests scan.
     * @param scanId          Id of scan that is associated with the current command.
     */
    public ScanInitCommand(
            @NotNull String requesterNodeId,
            @NotNull IgniteUuid scanId
    ) {
        this.requesterNodeId = requesterNodeId;
        this.scanId = scanId;
    }

    /**
     * @return Id of the node that requests scan.
     */
    public @NotNull String requesterNodeId() {
        return requesterNodeId;
    }

    /**
     * @return Id of scan that is associated with the current command.
     */
    @NotNull
    public IgniteUuid scanId() {
        return scanId;
    }
}
