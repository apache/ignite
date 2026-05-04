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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/** */
public class ChangeGlobalStateFinishMessage extends DiscoveryCustomMessage {
    /** State change request ID. */
    @Order(0)
    UUID reqId;

    /** New cluster state. */
    @Order(1)
    ClusterState state;

    /** State change error. */
    @Order(2)
    boolean transitionRes;

    /** Constructor. */
    public ChangeGlobalStateFinishMessage() {
        // No-op.
    }

    /**
     * @param reqId State change request ID.
     * @param state New cluster state.
     * @param transitionRes State change error.
     */
    public ChangeGlobalStateFinishMessage(
        UUID reqId,
        ClusterState state,
        boolean transitionRes
    ) {
        super(IgniteUuid.randomUuid());

        assert reqId != null;
        assert state != null;

        this.reqId = reqId;
        this.state = state;
        this.transitionRes = transitionRes;
    }

    /**
     * @return State change request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return New cluster state.
     * @deprecated Use {@link #state()} instead.
     */
    @Deprecated
    public boolean clusterActive() {
        return state.active();
    }

    /**
     * @return Transition success status.
     */
    public boolean success() {
        return transitionRes;
    }

    /**
     * @return New cluster state.
     */
    public ClusterState state() {
        return state;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeGlobalStateFinishMessage.class, this);
    }
}
