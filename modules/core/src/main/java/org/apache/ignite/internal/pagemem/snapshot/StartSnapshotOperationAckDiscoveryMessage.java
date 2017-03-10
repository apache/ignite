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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a snapshot has been started.
 */
public class StartSnapshotOperationAckDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;


    private SnapshotOperation snapshotOperation;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Operation id. */
    private IgniteUuid opId;

    /** */
    private Exception err;

    /** */
    private UUID initiatorNodeId;

    /** Last full snapshot id for cache. */
    private Map<Integer, Long> lastFullSnapshotIdForCache;

    /** Last snapshot id for cache. */
    private Map<Integer, Long> lastSnapshotIdForCache;

    /**
     * @param snapshotOperation Snapshot Operation.
     * @param err Error.
     */
    public StartSnapshotOperationAckDiscoveryMessage(
        IgniteUuid id,
        SnapshotOperation snapshotOperation,
        Map<Integer, Long> lastFullSnapshotIdForCache,
        Map<Integer, Long> lastSnapshotIdForCache,
        Exception err,
        UUID initiatorNodeId
    ) {
        this.opId = id;
        this.snapshotOperation = snapshotOperation;
        this.lastFullSnapshotIdForCache = lastFullSnapshotIdForCache;
        this.lastSnapshotIdForCache = lastSnapshotIdForCache;
        this.err = err;
        this.initiatorNodeId = initiatorNodeId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     *
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @return Initiator node id.
     */
    public UUID initiatorNodeId() {
        return initiatorNodeId;
    }

    /**
     * @return Error if start this process is not successfully.
     */
    public Exception error() {
        return err;
    }

    /**
     * @return {@code True} if message has error otherwise {@code false}.
     */
    public boolean hasError() {
        return err != null;
    }

    public SnapshotOperation snapshotOperation() {
        return snapshotOperation;
    }

    /**
     * @param cacheId Cache id.
     */
    @Nullable public Long lastFullSnapshotId(int cacheId) {
        return lastFullSnapshotIdForCache.get(cacheId);
    }

    /**
     * @param cacheId Cache id.
     */
    @Nullable public Long lastSnapshotId(int cacheId) {
        return lastSnapshotIdForCache.get(cacheId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartSnapshotOperationAckDiscoveryMessage.class, this);
    }
}
