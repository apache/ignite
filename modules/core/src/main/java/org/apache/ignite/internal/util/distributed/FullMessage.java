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

package org.apache.ignite.internal.util.distributed;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Full process message. Contains single nodes results.
 *
 * @param <R> Result type.
 * @see DistributedProcess
 * @see InitMessage
 * @see SingleNodeMessage
 */
public class FullMessage<R extends Serializable> implements DiscoveryCustomMessage, Message {
    /** */
    public static final short DIRECT_TYPE = 513;

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    @Order(0)
    IgniteUuid id;

    /** Process id. */
    @Order(1)
    UUID processId;

    /** Process type. */
    @Order(2)
    int type;

    /** Results. */
    // TODO: Generic type R extends Serializable may require special handling in the future.
    @Order(3)
    Map<UUID, R> res;

    /** Errors. */
    // TODO: Throwable is not a standard Message type, may require special handling in the future.
    @Order(4)
    Map<UUID, Throwable> err;

    /** */
    public FullMessage() {
        // No-op.
    }

    /**
     * @param processId Process id.
     * @param type Process type.
     * @param res Results.
     * @param err Errors
     */
    public FullMessage(UUID processId, DistributedProcessType type, Map<UUID, R> res, Map<UUID, Throwable> err) {
        this.id = IgniteUuid.randomUuid();
        this.processId = processId;
        this.type = type.ordinal();
        this.res = res;
        this.err = err;
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
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** @return Nodes results. */
    public Map<UUID, R> result() {
        return res;
    }

    /** @return Nodes errors. */
    public Map<UUID, Throwable> error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FullMessage.class, this);
    }
}
