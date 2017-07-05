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

package org.apache.ignite.internal.processors.cluster;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ChangeGlobalStateFinishMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** State change request ID. */
    private UUID reqId;

    /** New cluster state. */
    private boolean clusterActive;

    /**
     * @param reqId State change request ID.
     * @param clusterActive New cluster state.
     */
    public ChangeGlobalStateFinishMessage(UUID reqId, boolean clusterActive) {
        assert reqId != null;

        this.reqId = reqId;
        this.clusterActive = clusterActive;
    }

    /**
     * @return State change request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return New cluster state.
     */
    public boolean clusterActive() {
        return clusterActive;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeGlobalStateFinishMessage.class, this);
    }
}
