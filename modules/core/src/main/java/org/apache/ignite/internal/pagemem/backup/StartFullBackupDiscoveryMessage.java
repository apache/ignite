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

package org.apache.ignite.internal.pagemem.backup;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a backup has been started.
 */
public class StartFullBackupDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** Backup ID. */
    private long globalBackupId;

    /** */
    private Collection<String> cacheNames;

    /** */
    private UUID initiatorId;

    /** Error. */
    private Exception err;

    private boolean incremental;

    private Map<Integer, Long> lastFullBackupIdForCache = new HashMap<>();

    /**
     * @param cacheNames Cache names.
     */
    public StartFullBackupDiscoveryMessage(long globalBackupId, Collection<String> cacheNames, UUID initiatorId, boolean incremental) {
        this.globalBackupId = globalBackupId;
        this.cacheNames = cacheNames;
        this.initiatorId = initiatorId;
        this.incremental = incremental;
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
    public long globalBackupId() {
        return globalBackupId;
    }

    /**
     * @return Cache names.
     */
    public Collection<String> cacheNames() {
        return cacheNames;
    }

    public boolean incremental() {
        return incremental;
    }

    public Long lastFullBackupId(int cacheId) {
        return lastFullBackupIdForCache.get(cacheId);
    }

    public void lastFullBackupId(int cacheId, long id) {
        lastFullBackupIdForCache.put(cacheId, id);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return new StartFullBackupAckDiscoveryMessage(globalBackupId, incremental, lastFullBackupIdForCache, cacheNames, err, initiatorId);
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }
}
