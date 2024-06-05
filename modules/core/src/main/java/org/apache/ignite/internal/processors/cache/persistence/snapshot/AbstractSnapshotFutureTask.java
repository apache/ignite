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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * @param <T> Type of snapshot processing result.
 */
public abstract class AbstractSnapshotFutureTask<T> extends GridFutureAdapter<T> {
    /** Snapshot operation request ID. */
    protected final UUID reqId;

    /** Unique identifier of snapshot process. */
    protected final String snpName;

    /** Node id which cause snapshot operation. */
    protected final UUID srcNodeId;

    /** */
    @GridToStringExclude
    private final AtomicBoolean started = new AtomicBoolean();    /** Ignite logger. */

    /**
     * Ctor.
     *
     * @param reqId Snapshot operation request id.
     * @param snpName Snapshot name.
     * @param srcNodeId Snapshot operation originator node id.
     */
    protected AbstractSnapshotFutureTask(UUID reqId, String snpName, UUID srcNodeId) {
        this.reqId = reqId;
        this.snpName = snpName;
        this.srcNodeId = srcNodeId;
    }

    /**
     * Initiates snapshot task.
     *
     * @return {@code true} if task started by this call.
     */
    public final boolean start() {
        return !isDone() && started.compareAndSet(false, true) && doStart();
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Node id which triggers this operation.
     */
    public UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Snapshot operation request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /** */
    protected abstract boolean doStart();

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    public void acceptException(Throwable th) {
        assert th != null;

        onDone(null, th, false);
    }

    /** {@inheritDoc} */
    @Override public final boolean cancel() {
        return onDone(null, new IgniteFutureCancelledCheckedException("Snapshot operation has been cancelled " +
            "by external process [snpName=" + snpName + ']'), false);
    }

    /** */
    @Override public String toString() {
        return S.toString(AbstractSnapshotFutureTask.class, this);
    }
}
