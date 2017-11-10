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

package org.apache.ignite.spi.discovery.zk;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/**
 *
 */
public class ZookeeperClusterNode implements ClusterNode, Serializable {
    /** */
    private UUID id;

    /** */
    private Serializable consistentId;

    /** */
    private long order;

    /** */
    private IgniteProductVersion ver;

    /** Node attributes. */
    @GridToStringExclude
    private Map<String, Object> attrs;

    /** */
    private transient boolean loc;

    /** TODO ZK */
    private transient ClusterMetrics metrics;

    /** */
    private boolean client;

    /**
     * @param id Node ID.
     * @param ver Node version.
     * @param attrs Node attributes.
     * @param consistentId Consistent ID.
     * @param client Client node flag.
     */
    public ZookeeperClusterNode(UUID id,
        IgniteProductVersion ver,
        Map<String, Object> attrs,
        Serializable consistentId,
        boolean client) {
        assert id != null;
        assert consistentId != null;

        this.id = id;
        this.ver = ver;
        this.attrs = U.sealMap(attrs);
        this.consistentId = consistentId;
        this.client = client;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /**
     * Sets consistent globally unique node ID which survives node restarts.
     *
     * @param consistentId Consistent globally unique node ID.
     */
    public void setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        final Map<String, Object> map = new HashMap<>(attrs);

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        attrs = Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T attribute(String name) {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        if (IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(name))
            return null;

        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        if (metrics == null)
            metrics = new ClusterMetricsSnapshot();

        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        // Even though discovery SPI removes this attribute after authentication, keep this check for safety.
        return F.view(attrs, new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return !IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS.equals(s);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order;
    }

    /**
     * @param order Order of the node.
     */
    public void order(long order) {
        assert order > 0 : order;

        this.order = order;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /**
     * @param loc Local node flag.
     */
    void local(boolean loc) {
        this.loc = loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return client;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return F.eqNodes(this, obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZookeeperClusterNode [id=" + id + ", order=" + order + ']';
    }
}
