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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Current snapshot operation on local node.
 */
public class SnapshotOperation {
    /** */
    private final SnapshotOperationRequest req;

    /** Snapshot file tree. */
    @GridToStringExclude
    private final SnapshotFileTree sft;

    /** Snapshot metadata. */
    @GridToStringExclude
    private SnapshotMetadata meta;

    /** Exception occurred during snapshot operation processing. */
    private volatile Throwable err;

    /** Warning flag of concurrent inconsistent-by-nature streamer updates. */
    @GridToStringExclude
    private volatile boolean streamerWrn;

    /**
     * Snapshot operation warnings. Warnings do not interrupt snapshot process but raise exception at the end to make
     * the operation status 'not OK' if no other error occurred.
     */
    private volatile List<String> warnings;

    /** Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed. */
    private volatile boolean startStageEnded;

    /** */
    public SnapshotOperation(SnapshotOperationRequest req, SnapshotFileTree sft) {
        this.req = req;
        this.sft = sft;
    }

    /** */
    public SnapshotOperationRequest request() {
        return req;
    }

    /** @return Snapshot file tree. */
    public SnapshotFileTree snapshotFileTree() {
        return sft;
    }

    /** @return Snapshot metadata. */
    public SnapshotMetadata meta() {
        return meta;
    }

    /** Stores snapshot metadata. */
    public void meta(SnapshotMetadata meta) {
        this.meta = meta;
    }

    /** @return Exception occurred during snapshot operation processing. */
    public Throwable error() {
        return err;
    }

    /** @param err Exception occurred during snapshot operation processing. */
    public void error(Throwable err) {
        this.err = err;
    }

    /** {@code True} If the streamer warning flag is set. {@code False} otherwise. */
    public boolean streamerWarning() {
        return streamerWrn;
    }

    /** Sets the streamer warning flag. */
    public boolean streamerWarning(boolean val) {
        return streamerWrn = val;
    }

    /** @return Warnings of snapshot operation. */
    public List<String> warnings() {
        return warnings;
    }

    /** @param warnings Warnings of snapshot operation. */
    public void warnings(List<String> warnings) {
        assert this.warnings == null;

        this.warnings = warnings;
    }

    /**
     * @return Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed.
     */
    protected boolean startStageEnded() {
        return startStageEnded;
    }

    /**
     * @param startStageEnded Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed.
     */
    protected void startStageEnded(boolean startStageEnded) {
        this.startStageEnded = startStageEnded;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotOperation.class, this, super.toString());
    }
}
