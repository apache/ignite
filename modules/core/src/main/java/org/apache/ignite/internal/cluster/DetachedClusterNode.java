/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 *
 */

package org.apache.ignite.internal.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Representation of cluster node that isn't currently present in cluster.
 */
public class DetachedClusterNode implements ClusterNode {
    /** */
    @GridToStringExclude
    private final UUID uuid = UUID.randomUUID();

    /** Consistent ID. */
    @GridToStringInclude
    private final Object consistentId;

    /** Node attributes. */
    @GridToStringInclude
    private final Map<String, Object> attributes;

    /**
     * @param consistentId Consistent ID.
     * @param attributes Node attributes.
     */
    public DetachedClusterNode(Object consistentId, Map<String, Object> attributes) {
        this.consistentId = consistentId;
        this.attributes = attributes;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return uuid;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T attribute(String name) {
        return (T)attributes.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public long order() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throw new UnsupportedOperationException("Operation is not supported on DetachedClusterNode");
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DetachedClusterNode.class, this);
    }
}
