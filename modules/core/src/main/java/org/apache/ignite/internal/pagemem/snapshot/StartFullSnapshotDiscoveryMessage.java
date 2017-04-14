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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a snapshot has been started.
 */
public class StartFullSnapshotDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private final String msg;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Snapshot ID. */
    private long globalSnapshotId;

    /** */
    private Collection<String> cacheNames;

    /** */
    private UUID initiatorId;

    /** Error. */
    private Exception err;

    /** Full snapshot. */
    private boolean fullSnapshot;

    /** Last full snapshot id for cache. */
    private Map<Integer, Long> lastFullSnapshotIdForCache = new HashMap<>();

    /**
     * @param cacheNames Cache names.
     * @param msg message to log
     */
    public StartFullSnapshotDiscoveryMessage(long globalSnapshotId, Collection<String> cacheNames, UUID initiatorId,
        boolean fullSnapshot, String msg) {
        this.globalSnapshotId = globalSnapshotId;
        this.cacheNames = cacheNames;
        this.initiatorId = initiatorId;
        this.fullSnapshot = fullSnapshot;
        this.msg = msg;
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
     * @return Backup ID.
     */
    public long globalSnapshotId() {
        return globalSnapshotId;
    }

    /**
     * @return Cache names.
     */
    public Collection<String> cacheNames() {
        return cacheNames;
    }

    /**
     *
     */
    public boolean fullSnapshot() {
        return fullSnapshot;
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

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return new StartFullSnapshotAckDiscoveryMessage(globalSnapshotId, fullSnapshot, lastFullSnapshotIdForCache, cacheNames, err, initiatorId, msg);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /**
     * @param full full snapshot.
     */
    public void fullSnapshot(boolean full) {
        fullSnapshot = full;
    }
}
