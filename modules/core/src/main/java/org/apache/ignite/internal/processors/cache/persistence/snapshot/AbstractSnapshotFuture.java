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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * @param <T> Type of snapshot processing result.
 */
abstract class AbstractSnapshotFuture<T> extends GridFutureAdapter<T> {
    /** Ignite logger. */
    @Nullable protected final IgniteLogger log;

    /** Snapshot operation request ID. */
    protected final UUID reqId;

    /** Unique identifier of snapshot process. */
    protected final String snpName;

    /** Unique identifier of snapshot process. */
    @Nullable protected final UUID srcNodeId;

    /**
     * @param reqId     Snapshot operation request ID.
     * @param snpName   Unique identifier of snapshot or snapshot process.
     * @param srcNodeId Node id which cause snapshot operation.
     */
    protected AbstractSnapshotFuture(
        @Nullable IgniteLogger log,
        UUID reqId,
        String snpName,
        @Nullable UUID srcNodeId
    ) {
        this.log = log;
        this.srcNodeId = srcNodeId;
        this.reqId = reqId;
        this.snpName = snpName;
    }

    /**
     * @return Snapshot name.
     */
    public final String snapshotName() {
        return snpName;
    }

    /**
     * @return Node id which triggers this operation.
     */
    @Nullable public final UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Snapshot operation request ID.
     */
    public final UUID requestId() {
        return reqId;
    }

    /**
     * Initiates snapshot task.
     *
     * @return {@code true} if task started by this call.
     */
    public abstract boolean start();

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    public void acceptException(Throwable th) {
        onDone(th);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        // Cancellation of snapshot future should not throw an exception.
        acceptException(new IgniteFutureCancelledCheckedException("Snapshot operation has been cancelled " +
            "by external process [snpName=" + snpName + ']'));

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotFuture.class, this);
    }
}
