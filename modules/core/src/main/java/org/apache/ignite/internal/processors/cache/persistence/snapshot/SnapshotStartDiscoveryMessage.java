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
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;

/** Snapshot operation start message. */
public class SnapshotStartDiscoveryMessage extends InitMessage<SnapshotOperationRequest> implements SnapshotDiscoveryMessage {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    boolean needExchange;

    /** Default constructor for {@link MessageFactory}. */
    public SnapshotStartDiscoveryMessage() {
        // No-op.
    }

    /**
     * @param procId Unique process id.
     * @param req Snapshot initial request.
     */
    public SnapshotStartDiscoveryMessage(UUID procId, SnapshotOperationRequest req) {
        super(procId, START_SNAPSHOT, req, req.incremental());

        needExchange = !req.incremental();
    }

    /** {@inheritDoc} */
    @Override public boolean needExchange() {
        return needExchange;
    }

    /** {@inheritDoc} */
    @Override public boolean needAssignPartitions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotStartDiscoveryMessage.class, this, super.toString());
    }
}
