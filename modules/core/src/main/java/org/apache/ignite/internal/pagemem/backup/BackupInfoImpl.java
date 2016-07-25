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
import org.apache.ignite.internal.util.future.GridFutureAdapter;

public class BackupInfoImpl implements BackupInfo {

    private final long backupId;

    private final UUID initiatorNode;

    private final Collection<String> cacheNames;

    private final GridFutureAdapter initFut = new GridFutureAdapter();

    public BackupInfoImpl(long backupId, UUID initiatorNode, Collection<String> cacheNames) {
        this.backupId = backupId;
        this.initiatorNode = initiatorNode;
        this.cacheNames = cacheNames;
    }

    @Override public long backupId() {
        return backupId;
    }

    @Override public UUID initiatorNode() {
        return initiatorNode;
    }

    @Override public Collection<String> cacheNames() {
        return cacheNames;
    }

    @Override public boolean initiator() {
        return false;
    }

    @Override public GridFutureAdapter initFut() {
        return initFut;
    }
}
