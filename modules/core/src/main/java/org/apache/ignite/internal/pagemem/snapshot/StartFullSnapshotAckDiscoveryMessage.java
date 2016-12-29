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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a snapshot has been started.
 */
public class StartFullSnapshotAckDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private final String msg;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private long globalSnapshotId;

    /** */
    private Exception err;

    /** */
    private Collection<String> cacheNames;

    /** */
    private UUID initiatorNodeId;

    /** Full snapshot. */
    private boolean fullSnapshot;

    /** Last full snapshot id for cache. */
    private Map<Integer, Long> lastFullSnapshotIdForCache;

    /**
     * @param globalSnapshotId Snapshot ID.
     * @param err Error.
     * @param cacheNames Cache names.
     */
    public StartFullSnapshotAckDiscoveryMessage(long globalSnapshotId, boolean fullSnapshot,
        Map<Integer, Long> lastFullSnapshotIdForCache,
        Collection<String> cacheNames, Exception err,
        UUID initiatorNodeId, String msg) {
        this.globalSnapshotId = globalSnapshotId;
        this.fullSnapshot = fullSnapshot;
        this.lastFullSnapshotIdForCache = lastFullSnapshotIdForCache;
        this.err = err;
        this.cacheNames = cacheNames;
        this.initiatorNodeId = initiatorNodeId;
        this.msg = msg;
    }

    /**
     * @return Cache names.
     */
    public Collection<String> cacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
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

    /**
     * @return Snapshot ID.
     */
    public long globalSnapshotId() {
        return globalSnapshotId;
    }

    /**
     *
     */
    public boolean fullSnapshot() {
        return fullSnapshot;
    }

    /**
     *
     */
    public String message() {
        return msg;
    }

    /**
     * @param cacheId Cache id.
     */
    @Nullable public Long lastFullSnapshotId(int cacheId) {
        return lastFullSnapshotIdForCache.get(cacheId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }
}
