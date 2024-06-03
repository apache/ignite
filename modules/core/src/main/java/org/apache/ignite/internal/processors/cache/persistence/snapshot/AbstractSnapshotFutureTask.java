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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class AbstractSnapshotFutureTask<T> extends GridFutureAdapter<T> {
    /** Ignite logger. */
    @GridToStringExclude
    @Nullable protected final IgniteLogger log;

    /** Node id which cause snapshot operation. */
    protected final UUID srcNodeId;

    /** Snapshot operation request ID. */
    protected final UUID reqId;

    /** Unique identifier of snapshot process. */
    protected final String snpName;

    /** */
    @GridToStringExclude
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * Ctor.
     * @param log Logger.
     * @param srcNodeId Snapshot operation originator node id.
     * @param reqId Snapshot operation request id.
     * @param snpName Snapshot name.
     */
    protected AbstractSnapshotFutureTask(@Nullable IgniteLogger log, UUID srcNodeId, UUID reqId, String snpName) {
        this.log = log;
        this.srcNodeId = srcNodeId;
        this.reqId = reqId;
        this.snpName = snpName;
    }

    /**
     * Initiates snapshot task.
     *
     * @return {@code true} if task started by this call.
     * @see #doStart()
     */
    public final boolean start() {
        return !isDone() && started.compareAndSet(false, true) && doStart();
    }

    /**
     * Is called once when the future starts.
     *
     * @see #start()
     */
    protected abstract boolean doStart();

    /**
     * Is called once when the future stops.
     * @see #onDone(Object, Throwable, boolean)
     * @see #onDone(Object, Throwable)
     * @see #acceptException(Throwable)
     */
    protected abstract boolean doStop();

    /**
     * @param th An exception which occurred during snapshot processing.
     * @see #doStop()
     */
    public final boolean acceptException(Throwable th) {
        assert th != null;

        return onDone(null, th, th instanceof IgniteFutureCancelledCheckedException);
    }

    /** {@inheritDoc} */
    @Override protected final boolean onDone(@Nullable T res, @Nullable Throwable err, boolean cancel) {
        return super.onDone(res, err, cancel) && doStop();
    }

    /** {@inheritDoc} */
    @Override public final boolean onDone(@Nullable T res, @Nullable Throwable err) {
        return super.onDone(res, err) && doStop();
    }

    /** {@inheritDoc} */
    @Override public final boolean cancel() {
        return acceptException(new IgniteFutureCancelledCheckedException("Snapshot operation has been cancelled " +
            "by external process [snpName=" + snpName + ']'));
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
    public final UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Snapshot operation request ID.
     */
    public final UUID requestId() {
        return reqId;
    }

    /** */
    @Override public String toString() {
        return S.toString(AbstractSnapshotFutureTask.class, this);
    }
}
