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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client implementation of {@link ClusterNode}.
 */
class ClientClusterNodeImpl implements ClusterNode {
    /** Node id. */
    private final UUID id;

    /** Consistent id. */
    private final Object consistentId;

    /** Attributes. */
    private final Map<String, Object> attrs;

    /** Addresses. */
    private final Collection<String> addresses;

    /** Host names. */
    private final Collection<String> hostNames;

    /** Order. */
    private final long order;

    /** Version. */
    private final IgniteProductVersion ver;

    /** Is local. For thin-client this flag is {@code true} for node to which client is connected. */
    private final boolean isLoc;

    /** Is daemon. */
    private final boolean isDaemon;

    /** Is client. */
    private final boolean isClient;

    /**
     * Default constructor.
     */
    ClientClusterNodeImpl(
        UUID id,
        Map<String, Object> attrs,
        Collection<String> addresses,
        Collection<String> hostNames,
        long order,
        boolean isLoc,
        boolean isDaemon,
        boolean isClient,
        Object consistentId,
        IgniteProductVersion ver
    ) {
        this.id = id;
        this.consistentId = consistentId;
        this.attrs = Collections.unmodifiableMap(attrs);
        this.addresses = Collections.unmodifiableCollection(addresses);
        this.hostNames = Collections.unmodifiableCollection(hostNames);
        this.order = order;
        this.ver = ver;
        this.isLoc = isLoc;
        this.isDaemon = isDaemon;
        this.isClient = isClient;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        throw new UnsupportedOperationException("Metrics are not supported for thin client implementation of ClusterNode");
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return addresses;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return isLoc;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return isDaemon;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return isClient;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return F.eqNodes(this, o);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientClusterNodeImpl.class, this);
    }
}
