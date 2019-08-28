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

package org.apache.ignite.spi.metric.list.walker;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.spi.metric.MonitoringRowAttributeWalker;
import org.apache.ignite.spi.metric.list.CacheGroupView;

/** */
public class CacheGroupViewWalker implements MonitoringRowAttributeWalker<CacheGroupView> {

    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "topologyValidator", String.class);
        v.accept(1, "atomicityMode", CacheAtomicityMode.class);
        v.accept(2, "affinity", String.class);
        v.accept(3, "partitions", Integer.class);
        v.accept(4, "nodeFilter", String.class);
        v.accept(5, "dataRegionName", String.class);
        v.accept(6, "partitionLossPolicy", PartitionLossPolicy.class);
        v.accept(7, "rebalanceMode", CacheRebalanceMode.class);
        v.acceptLong(8, "rebalanceDelay");
        v.acceptInt(9, "rebalanceOrder");
        v.accept(10, "backups", Integer.class);
        v.acceptInt(11, "groupId");
        v.accept(12, "groupName", String.class);
        v.acceptBoolean(13, "sharedGroup");
        v.acceptInt(14, "cacheCount");
        v.accept(15, "cacheMode", CacheMode.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAllWithValues(CacheGroupView row, AttributeWithValueVisitor v) {
        v.accept(0, "topologyValidator", String.class, row.topologyValidator());
        v.accept(1, "atomicityMode", CacheAtomicityMode.class, row.atomicityMode());
        v.accept(2, "affinity", String.class, row.affinity());
        v.accept(3, "partitions", Integer.class, row.partitions());
        v.accept(4, "nodeFilter", String.class, row.nodeFilter());
        v.accept(5, "dataRegionName", String.class, row.dataRegionName());
        v.accept(6, "partitionLossPolicy", PartitionLossPolicy.class, row.partitionLossPolicy());
        v.accept(7, "rebalanceMode", CacheRebalanceMode.class, row.rebalanceMode());
        v.acceptLong(8, "rebalanceDelay", row.rebalanceDelay());
        v.acceptInt(9, "rebalanceOrder", row.rebalanceOrder());
        v.accept(10, "backups", Integer.class, row.backups());
        v.acceptInt(11, "groupId", row.groupId());
        v.accept(12, "groupName", String.class, row.groupName());
        v.acceptBoolean(13, "sharedGroup", row.sharedGroup());
        v.acceptInt(14, "cacheCount", row.cacheCount());
        v.accept(15, "cacheMode", CacheMode.class, row.cacheMode());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 16;
    }
}
