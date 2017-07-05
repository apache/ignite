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

package org.apache.ignite.internal.pagemem.snapshot;

import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class FinishSnapshotOperationAckDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Id. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Op id. */
    private final IgniteUuid opId;

    /** Success. */
    private final boolean success;

    /**
     * @param opId Op id.
     * @param success Success.
     */
    public FinishSnapshotOperationAckDiscoveryMessage(IgniteUuid opId, boolean success) {
        this.opId = opId;
        this.success = success;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /**
     * @return Op id.
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @return Success.
     */
    public boolean success() {
        return success;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FinishSnapshotOperationAckDiscoveryMessage.class, this,
            "id", id, "opId", opId, "success", success);
    }
}
