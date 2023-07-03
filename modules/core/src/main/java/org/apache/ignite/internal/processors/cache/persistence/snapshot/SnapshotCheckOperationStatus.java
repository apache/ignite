/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Snapshot check operation status. */
public abstract class SnapshotCheckOperationStatus implements AutoCloseable {
    /** Snapshot metadata. */
    private final SnapshotMetadata meta;

    /** Operation request ID. */
    private final UUID reqId;

    /** Start time. */
    private final long startTime = U.currentTimeMillis();

    /** Processed partitions. */
    private final AtomicInteger processedParts = new AtomicInteger();

    /** Total partitions. */
    private final int totalParts;

    /** */
    SnapshotCheckOperationStatus(SnapshotMetadata meta, int totalParts, UUID reqId) {
        this.meta = meta;
        this.totalParts = totalParts;
        this.reqId = reqId;
    }

    /** @return Snapshot metadata. */
    public SnapshotMetadata metadata() {
        return meta;
    }

    /** @return Operation request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Processed partitions. */
    public int processedPartitions() {
        return processedParts.get();
    }

    /** @return Total partitions. */
    public int totalPartitions() {
        return totalParts;
    }

    /** */
    public void onPartitionProcessed() {
        processedParts.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotCheckOperationStatus status = (SnapshotCheckOperationStatus)o;

        return reqId.equals(status.reqId) && meta.equals(status.meta);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(reqId, meta);
    }
}
