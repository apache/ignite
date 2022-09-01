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

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * @param <T> Type of snapshot processing result.
 */
abstract class AbstractSnapshotFutureTask<T> extends GridFutureAdapter<T> {
    /** Shared context. */
    protected final GridCacheSharedContext<?, ?> cctx;

    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Node id which cause snapshot operation. */
    protected final UUID srcNodeId;

    /** Unique snapshot operation ID. */
    protected final UUID operId;

    /** Unique identifier of snapshot process. */
    protected final String snpName;

    /** Snapshot working directory on file system. */
    protected final File tmpSnpWorkDir;

    /** IO factory which will be used for creating snapshot delta-writers. */
    protected final FileIOFactory ioFactory;

    /** Snapshot data sender. */
    @GridToStringExclude
    protected final SnapshotSender snpSndr;

    /** Partition to be processed. */
    protected final Map<Integer, Set<Integer>> parts;

    /** An exception which has been occurred during snapshot processing. */
    protected final AtomicReference<Throwable> err = new AtomicReference<>();

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param snpName Unique identifier of snapshot process.
     * @param tmpWorkDir Working directory for intermediate snapshot results.
     * @param ioFactory Factory to working with snapshot files.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @param parts Partition to be processed.
     */
    protected AbstractSnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID operId,
        String snpName,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        assert snpName != null : "Snapshot name cannot be empty or null.";
        assert snpSndr != null : "Snapshot sender which handles execution tasks must be not null.";
        assert snpSndr.executor() != null : "Executor service must be not null.";

        this.cctx = cctx;
        this.log = cctx.logger(AbstractSnapshotFutureTask.class);
        this.srcNodeId = srcNodeId;
        this.operId = operId;
        this.snpName = snpName;
        this.tmpSnpWorkDir = new File(tmpWorkDir, snpName);
        this.ioFactory = ioFactory;
        this.snpSndr = snpSndr;
        this.parts = parts;
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
     * @return Snapshot operation ID.
     */
    public UUID operationId() {
        return operId;
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
    public abstract void acceptException(Throwable th);

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        // Cancellation of snapshot future should not throw an exception.
        acceptException(new IgniteFutureCancelledCheckedException("Snapshot operation has been cancelled " +
            "by external process [snpName=" + snpName + ']'));

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotFutureTask.class, this);
    }
}
