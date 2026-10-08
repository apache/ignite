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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;

/** Accumulated result of {@link SnapshotListTask}. */
public final class SnapshotListTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Nodes consistent ids. */
    @Order(0)
    String[] cstIds;

    /** Nodes UUIDs. */
    @Order(1)
    UUID[] nodesIds;

    /** Results. */
    @Order(2)
    SnapshotListJobResult[] snapshots;

    /** Default constructor for serialization purposes. */
    public SnapshotListTaskResult() {
        // No-op.
    }

    /**  */
    public SnapshotListTaskResult(String[] cstIds, UUID[] nodesIds, SnapshotListJobResult[] snapshots) {
        this.cstIds = cstIds;
        this.nodesIds = nodesIds;
        this.snapshots = snapshots;
    }

    /** @return Nodes consistent ids. */
    public String[] consistentIds() {
        return cstIds;
    }

    /** @return Nodes UUIDs. */
    public UUID[] nodesIds() {
        return nodesIds;
    }

    /** @return The results. */
    public SnapshotListJobResult[] snapshots() {
        return snapshots;
    }
}
