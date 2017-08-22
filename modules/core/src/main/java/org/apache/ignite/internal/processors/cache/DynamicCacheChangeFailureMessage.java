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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents discovery message that is used to provide information about dynamic cache start failure.
 */
public class DynamicCacheChangeFailureMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Change requests. */
    @GridToStringInclude
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Custom message ID. */
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private GridDhtPartitionExchangeId exchId;

    /** */
    private GridCacheVersion lastVer;

    @GridToStringInclude
    private IgniteCheckedException cause;

    /**
     * Creates new DynamicCacheChangeFailureMessage instance.
     *
     * @param exchId Exchange Id.
     * @param lastVer Last version.
     * @param cause
     * @param reqs Cache change requests.
     */
    public DynamicCacheChangeFailureMessage(GridDhtPartitionExchangeId exchId,
        @Nullable GridCacheVersion lastVer,
        IgniteCheckedException cause,
        Collection<DynamicCacheChangeRequest> reqs) {
        this.exchId = exchId;
        this.lastVer = lastVer;
        this.cause = cause;
        this.reqs = reqs;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param id Message ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Collection of change requests.
     */
    public Collection<DynamicCacheChangeRequest> requests() {
        return reqs;
    }

    public IgniteCheckedException getError() {
        return cause;
    }

    /**
     * @return Last used version among all nodes.
     */
    @Nullable public GridCacheVersion lastVersion() {
        return lastVer;
    }

    /**
     * @return Exchange version.
     */
    @Nullable public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
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
        return S.toString(DynamicCacheChangeFailureMessage.class, this);
    }
}
