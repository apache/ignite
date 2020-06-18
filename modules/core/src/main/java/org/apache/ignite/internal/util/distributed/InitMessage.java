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
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Initiate message.
 *
 * @param <I> Request type.
 * @see DistributedProcess
 * @see FullMessage
 * @see SingleNodeMessage
 */
public class InitMessage<I extends Serializable> implements DiscoveryCustomMessage {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Custom message ID. */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Process id. */
    private final UUID processId;

    /** Process type. */
    private final int type;

    /** Request. */
    private final I req;

    /**
     * @param processId Process id.
     * @param type Process type.
     * @param req Request.
     */
    public InitMessage(UUID processId, DistributedProcessType type, I req) {
        this.processId = processId;
        this.type = type.ordinal();
        this.req = req;
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
        return mgr.createDiscoCacheOnCacheChange(topVer, discoCache);
    }

    /** @return Process id. */
    public UUID processId() {
        return processId;
    }

    /** @return Process type. */
    public int type() {
        return type;
    }

    /** @return Request. */
    public I request() {
        return req;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InitMessage.class, this);
    }
}
