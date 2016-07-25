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

public class BackupFuture extends GridFutureAdapter<Void> implements BackupInfo {

    private final long backupId;

    private final UUID node;

    private final Collection<String> cacheNames;

    private final GridFutureAdapter initFut = new GridFutureAdapter();

    private volatile boolean initialized;

    private final Collection<UUID> requiredAcks = new GridConcurrentHashSet<>();

    private final Collection<UUID> receivedAcks = new GridConcurrentHashSet<>();

    public BackupFuture(long backupId, UUID node, Collection<String> cacheNames) {
        this.backupId = backupId;
        this.node = node;
        this.cacheNames = cacheNames;
    }

    @Override public long backupId() {
        return backupId;
    }

    @Override public UUID initiatorNode() {
        return node;
    }

    @Override public Collection<String> cacheNames() {
        return cacheNames;
    }

    @Override public boolean initiator() {
        return true;
    }

    @Override public GridFutureAdapter initFut() {
        return initFut;
    }

    public void init(Collection<UUID> requiredAcks) {
        assert !initialized;

        this.requiredAcks.addAll(requiredAcks);

        initialized = true;

        checkCompleted();
    }

    public void onAckReceived(UUID nodeId) {
        receivedAcks.add(nodeId);

        checkCompleted();
    }

    private void checkCompleted() {
        if (initialized && receivedAcks.containsAll(requiredAcks))
            onDone();
    }

}
