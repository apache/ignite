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
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public class BackupMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    private long backupId;

    private Collection<String> cacheNames;

    /** Future which indicates that exchange for this backup had started. */
    private transient volatile GridFutureAdapter future;

    public BackupMessage(long backupId, Collection<String> cacheNames) {
        this.backupId = backupId;
        this.cacheNames = cacheNames;
    }

    @Override public IgniteUuid id() {
        return id;
    }

    public long backupId() {
        return backupId;
    }

    public Collection<String> cacheNames() {
        return cacheNames;
    }

    public GridFutureAdapter future() {
        return future;
    }

    public void future(GridFutureAdapter future) {
        this.future = future;
    }

    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    @Override public boolean isMutable() {
        return false;
    }
}
