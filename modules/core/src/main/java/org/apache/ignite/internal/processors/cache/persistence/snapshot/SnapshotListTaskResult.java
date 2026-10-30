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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

/** Accumulated result of the snapshot list task. */
public final class SnapshotListTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Node consistent IDs. */
    @Order(0)
    String[] consistentIds;

    /** Node UUIDs. */
    @Order(1)
    UUID[] nodesIds;

    /** Per-node results. */
    @Order(2)
    SnapshotListJobResult[] nodesSnapshots;

    /** Per-node errors. */
    @Order(3)
    @Nullable IgniteException[] errors;

    /** Default constructor for serialization purposes. */
    public SnapshotListTaskResult() {
        // No-op.
    }

    /** */
    public SnapshotListTaskResult(
        String[] consistentIds,
        UUID[] nodesIds,
        SnapshotListJobResult[] nodesSnapshots,
        @Nullable IgniteException[] errors
    ) {
        this.consistentIds = consistentIds;
        this.nodesIds = nodesIds;
        this.nodesSnapshots = nodesSnapshots;
        this.errors = errors;
    }

    /** @return Nodes consistent IDs. */
    public String[] consistentIds() {
        return consistentIds;
    }

    /** @return Nodes UUIDs. */
    public UUID[] nodesIds() {
        return nodesIds;
    }

    /** @return Per-node results. */
    public SnapshotListJobResult[] nodesSnapshots() {
        return nodesSnapshots;
    }

    /** @return Per-node errors. */
    public @Nullable IgniteException[] errors() {
        return errors;
    }
}
