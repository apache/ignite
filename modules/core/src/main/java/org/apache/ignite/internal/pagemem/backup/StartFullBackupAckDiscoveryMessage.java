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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message indicating that a backup has been started.
 */
public class StartFullBackupAckDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private long globalBackupId;

    /** */
    private Exception err;

    /** */
    private Collection<String> cacheNames;

    /** */
    private UUID initiatorNodeId;

    private boolean incremental;

    private Map<Integer, Long> lastFullBackupIdForCache;

    /**
     * @param globalBackupId Backup ID.
     * @param err Error.
     * @param cacheNames Cache names.
     */
    public StartFullBackupAckDiscoveryMessage(long globalBackupId, boolean incremental,
        Map<Integer, Long> lastFullBackupIdForCache,
        Collection<String> cacheNames, Exception err,
        UUID initiatorNodeId) {
        this.globalBackupId = globalBackupId;
        this.incremental = incremental;
        this.lastFullBackupIdForCache = lastFullBackupIdForCache;
        this.err = err;
        this.cacheNames = cacheNames;
        this.initiatorNodeId = initiatorNodeId;
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
     * @return Backup ID.
     */
    public long globalBackupId() {
        return globalBackupId;
    }

    public boolean incremental() {
        return incremental;
    }

    @Nullable public Long lastFullBackupId(int cacheId) {
        return lastFullBackupIdForCache.get(cacheId);
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
