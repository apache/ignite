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

package org.apache.ignite.spi.metric.list;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;

/** */
public class CacheGroupView implements MonitoringRow<String> {
    /** Cache group. */
    private CacheGroupDescriptor grp;

    /** Cache configuration. */
    private CacheConfiguration<?, ?> ccfg;

    /**
     * @param grp Cache group.
     */
    public CacheGroupView(CacheGroupDescriptor grp) {
        this.grp = grp;
        this.ccfg = grp.config();
    }

    /** */
    public int groupId() {
        return grp.groupId();
    }

    /** */
    public String groupName() {
        return grp.cacheOrGroupName();
    }

    /** */
    public boolean sharedGroup() {
        return grp.sharedGroup();
    }

    /** */
    public int cacheCount() {
        return F.size(grp.caches());
    }

    /** */
    public CacheMode cacheMode() {
        return ccfg.getCacheMode();
    }

    /** */
    public CacheAtomicityMode atomicityMode() {
        return ccfg.getAtomicityMode();
    }

    /** */
    public Class<?> affinity() {
        return ccfg.getAffinity() != null ? ccfg.getAffinity().getClass() : null;
    }

    /** */
    public int partitions() {
        return ccfg.getAffinity() != null ? ccfg.getAffinity().partitions() : -1;
    }

    /** */
    public Class<?> nodeFilter() {
        IgnitePredicate<ClusterNode> filter = ccfg.getNodeFilter();

        return filter == null ? null : filter.getClass();
    }

    /** */
    public String dataRegionName() {
        return ccfg.getDataRegionName();
    }

    /** */
    public Class<?> topologyValidator() {
        TopologyValidator validator = ccfg.getTopologyValidator();

        return validator == null ? null : validator.getClass();
    }

    /** */
    public PartitionLossPolicy partitionLossPolicy() {
        return ccfg.getPartitionLossPolicy();
    }

    /** */
    public CacheRebalanceMode rebalanceMode() {
        return ccfg.getRebalanceMode();
    }

    /** */
    public long rebalanceDelay() {
        return ccfg.getRebalanceDelay();
    }

    /** */
    public int rebalanceOrder() {
        return ccfg.getRebalanceOrder();
    }

    /** */
    public int backups() {
        return ccfg.getBackups();
    }
}
