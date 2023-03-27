/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.isolated;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;

/**
 * Special isolated node.
 */
public class IsolatedNode implements IgniteClusterNode {
    /** */
    private final UUID id;

    /** */
    private final IgniteProductVersion ver;

    /** Consistent ID. */
    private Object consistentId;

    /** Node attributes. */
    private Map<String, Object> attrs;

    /** */
    private volatile ClusterMetrics metrics = new ClusterMetricsSnapshot();

    /** */
    private volatile Map<Integer, CacheMetrics> cacheMetrics = Collections.emptyMap();

    /**
     * @param id Node ID.
     * @param attrs Node attributes.
     * @param ver Node version.
     */
    public IsolatedNode(UUID id, Map<String, Object> attrs, IgniteProductVersion ver) {
        this.id = id;
        this.attrs = U.sealMap(attrs);
        this.ver = ver;
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
    @Override public <T> T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return Collections.singleton("127.0.0.1");
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return Collections.singleton("localhost");
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        final Map<String, Object> map = new HashMap<>(attrs);

        map.put(ATTR_NODE_CONSISTENT_ID, consistentId);

        attrs = Collections.unmodifiableMap(map);
    }

    /** {@inheritDoc} */
    @Override public void setMetrics(ClusterMetrics metrics) {
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, CacheMetrics> cacheMetrics() {
        return cacheMetrics;
    }

    /** {@inheritDoc} */
    @Override public void setCacheMetrics(Map<Integer, CacheMetrics> cacheMetrics) {
        this.cacheMetrics = cacheMetrics != null ? cacheMetrics : Collections.emptyMap();
    }

    /** @param attrs Local node attributes. */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs = U.sealMap(attrs);
    }
}
