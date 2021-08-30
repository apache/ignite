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

package org.apache.ignite.spi.systemview.view;

import java.util.Map;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Cache group representation for the {@link SystemView}.
 */
public class CacheGroupView {
    /** Cache group. */
    private final CacheGroupDescriptor grp;

    /** Cache configuration. */
    private final CacheConfiguration<?, ?> ccfg;

    /**
     * @param grp Cache group.
     */
    public CacheGroupView(CacheGroupDescriptor grp) {
        this.grp = grp;
        this.ccfg = grp.config();
    }

    /** @return Cache group id. */
    public int cacheGroupId() {
        return grp.groupId();
    }

    /** @return Cache group name. */
    @Order()
    public String cacheGroupName() {
        return grp.cacheOrGroupName();
    }

    /** @return {@code True} if group shared, {@code false} otherwise. */
    public boolean isShared() {
        return grp.sharedGroup();
    }

    /** @return Cache count. */
    @Order(1)
    public int cacheCount() {
        Map<String, Integer> caches = grp.caches();

        return caches == null ? 0 : caches.size();
    }

    /** @return Cache mode. */
    @Order(3)
    public CacheMode cacheMode() {
        return ccfg.getCacheMode();
    }

    /** @return Atomicity mode. */
    @Order(4)
    public CacheAtomicityMode atomicityMode() {
        return ccfg.getAtomicityMode();
    }

    /** @return Affinity string representation. */
    public String affinity() {
        return ccfg.getAffinity() != null ? toStringSafe(ccfg.getAffinity()) : null;
    }

    /** @return Partitions count. */
    public int partitionsCount() {
        return ccfg.getAffinity() != null ? ccfg.getAffinity().partitions() : -1;
    }

    /** @return Node filter string representation. */
    public String nodeFilter() {
        return nodeFilter(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @return Node filter string representation.
     */
    public static String nodeFilter(CacheConfiguration<?, ?> ccfg) {
        IgnitePredicate<ClusterNode> nodeFilter = ccfg.getNodeFilter();

        if (nodeFilter instanceof CacheConfiguration.IgniteAllNodesPredicate)
            return null;

        return toStringSafe(nodeFilter);
    }

    /** @return Data region name. */
    @Order(2)
    public String dataRegionName() {
        return ccfg.getDataRegionName();
    }

    /** @return Topology validator. */
    public String topologyValidator() {
        TopologyValidator validator = ccfg.getTopologyValidator();

        return validator == null ? null : toStringSafe(validator);
    }

    /** @return Partition loss policy. */
    public PartitionLossPolicy partitionLossPolicy() {
        return ccfg.getPartitionLossPolicy();
    }

    /** @return Cache rebalance mode. */
    public CacheRebalanceMode rebalanceMode() {
        return ccfg.getRebalanceMode();
    }

    /** @return Rebalance delay. */
    public long rebalanceDelay() {
        return ccfg.getRebalanceDelay();
    }

    /** @return Rebalance order. */
    public int rebalanceOrder() {
        return ccfg.getRebalanceOrder();
    }

    /** @return Backups count. */
    public int backups() {
        return ccfg.getBackups();
    }
}
