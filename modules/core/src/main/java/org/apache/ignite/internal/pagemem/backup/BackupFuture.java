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
import java.util.UUID;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Backup future.
 */
public class BackupFuture extends GridFutureAdapter<Void> implements BackupInfo {
    /** Backup future ID. Timestamp when future was created. */
    private final long backupId;

    /** Originating node ID. */
    private final UUID node;

    /** Cases involved in full backup process. */
    private final Collection<String> cacheNames;

    /** Init future. */
    private final GridFutureAdapter initFut = new GridFutureAdapter();

    /** */
    private volatile boolean initialized;

    /** */
    private final Collection<UUID> requiredAcks = new GridConcurrentHashSet<>();

    /**
     * @param backupId Backup ID.
     * @param node Local node ID.
     * @param cacheNames Cache names.
     */
    public BackupFuture(long backupId, UUID node, Collection<String> cacheNames) {
        this.backupId = backupId;
        this.node = node;
        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    @Override public long backupId() {
        return backupId;
    }

    /** {@inheritDoc} */
    @Override public UUID initiatorNode() {
        return node;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override public boolean initiator() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter initFut() {
        return initFut;
    }

    /**
     * Marks future as initialized.
     *
     * @param requiredAcks Acks that are required for completion.
     */
    public void init(Collection<UUID> requiredAcks) {
        assert !initialized;

        this.requiredAcks.addAll(requiredAcks);

        initialized = true;

        checkCompleted();
    }

    /**
     * @param nodeId Ack sender.
     */
    public void onAckReceived(UUID nodeId) {
        assert initialized;

        requiredAcks.remove(nodeId);

        checkCompleted();
    }

    /**
     * Complete future if all requirements are fulfilled.
     */
    private void checkCompleted() {
        if (initialized && requiredAcks.isEmpty())
            onDone();
    }

}
