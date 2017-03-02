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

package org.apache.ignite.internal.pagemem.snapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a snapshot has been started.
 */
public class StartSnapshotOperationDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id;

    /** Snapshot operation. */
    private SnapshotOperation snapshotOperation;

    /** */
    private UUID initiatorId;

    /** Error. */
    private Exception err;

    /** Last full snapshot id for cache. */
    private Map<Integer, Long> lastFullSnapshotIdForCache = new HashMap<>();

    /** Last snapshot id for cache. */
    private Map<Integer, Long> lastSnapshotIdForCache = new HashMap<>();

    /**
     * @param snapshotOperation Snapshot operation
     * @param initiatorId initiator node id
     */
    public StartSnapshotOperationDiscoveryMessage(
        IgniteUuid id,
        SnapshotOperation snapshotOperation,
        UUID initiatorId
    ) {
        this.id = id;
        this.snapshotOperation = snapshotOperation;
        this.initiatorId = initiatorId;
    }

    /**
     *
     */
    public SnapshotOperation snapshotOperation() {
        return snapshotOperation;
    }

    /**
     * Sets error.
     *
     * @param err Error.
     */
    public void error(Exception err) {
        this.err = err;
    }

    /**
     * @return {@code True} if message contains error.
     */
    public boolean hasError() {
        return err != null;
    }

    /**
     * @return Error.
     */
    public Exception error() {
        return err;
    }

    /**
     * @return Initiator node id.
     */
    public UUID initiatorId() {
        return initiatorId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param cacheId Cache id.
     */
    public Long lastFullSnapshotId(int cacheId) {
        return lastFullSnapshotIdForCache.get(cacheId);
    }

    /**
     * @param cacheId Cache id.
     * @param id Id.
     */
    public void lastFullSnapshotId(int cacheId, long id) {
        lastFullSnapshotIdForCache.put(cacheId, id);
    }

    /**
     * @param cacheId Cache id.
     */
    public Long lastSnapshotId(int cacheId) {
        return lastSnapshotIdForCache.get(cacheId);
    }

    /**
     * @param cacheId Cache id.
     * @param id Id.
     */
    public void lastSnapshotId(int cacheId, long id) {
        lastSnapshotIdForCache.put(cacheId, id);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return new StartSnapshotOperationAckDiscoveryMessage(
            id,
            snapshotOperation,
            lastFullSnapshotIdForCache,
            lastSnapshotIdForCache,
            err,
            initiatorId);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /**
     * @param snapshotOperation new snapshot operation
     */
    public void snapshotOperation(SnapshotOperation snapshotOperation) {
        this.snapshotOperation = snapshotOperation;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartSnapshotOperationDiscoveryMessage.class, this);
    }
}
