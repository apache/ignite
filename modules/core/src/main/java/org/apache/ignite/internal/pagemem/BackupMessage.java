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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

public class BackupMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    private transient GridCompoundFuture future;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    private long backupId;

    public BackupMessage(long backupId) {
        this.backupId = backupId;
    }

    @Override public IgniteUuid id() {
        return id;
    }

    public long backupId() {
        return backupId;
    }

    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    @Override public boolean isMutable() {
        return false;
    }

    public GridCompoundFuture future() {
        return future;
    }

    public void future(GridCompoundFuture future) {
        this.future = future;
    }
}
