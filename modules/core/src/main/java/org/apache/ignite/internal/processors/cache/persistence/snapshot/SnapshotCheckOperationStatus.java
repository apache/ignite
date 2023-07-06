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
    /** Operation request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** Incremental snapshot index. */
    private final int incIdx;

    /** Start time. */
    private final long startTime = U.currentTimeMillis();

    /** Processed count. */
    private final AtomicInteger processed = new AtomicInteger();

    /** Total count. */
    private final AtomicInteger total = new AtomicInteger();

    /** */
    SnapshotCheckOperationStatus(UUID reqId, String snpName, int incIdx) {
        this.reqId = reqId;
        this.snpName = snpName;
        this.incIdx = incIdx;
    }

    /** @return Operation request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Incremental snapshot index. */
    public int incrementIndex() {
        return incIdx;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return Processed count. */
    public int processed() {
        return processed.get();
    }

    /** @return Total count. */
    public int total() {
        return total.get();
    }

    /** Sets total count. */
    public void total(int total) {
        this.total.set(total);
    }

    /** Sets processed count. */
    public void processed(int processed) {
        this.processed.set(processed);
    }

    /** */
    public void onProcessed() {
        processed.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotCheckOperationStatus status = (SnapshotCheckOperationStatus)o;

        return reqId.equals(status.reqId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(reqId);
    }
}
