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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: cache groups.
 */
public class SqlSystemViewCacheGroups extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewCacheGroups(GridKernalContext ctx) {
        super("CACHE_GROUPS", "Cache groups", ctx, "CACHE_GROUP_ID",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("CACHE_GROUP_NAME"),
            newColumn("IS_SHARED", Value.BOOLEAN),
            newColumn("CACHE_COUNT", Value.INT),
            newColumn("CACHE_MODE"),
            newColumn("ATOMICITY_MODE"),
            newColumn("AFFINITY"),
            newColumn("PARTITIONS_COUNT", Value.INT),
            newColumn("NODE_FILTER"),
            newColumn("DATA_REGION_NAME"),
            newColumn("TOPOLOGY_VALIDATOR"),
            newColumn("PARTITION_LOSS_POLICY"),
            newColumn("REBALANCE_MODE"),
            newColumn("REBALANCE_DELAY", Value.LONG),
            newColumn("REBALANCE_ORDER", Value.INT),
            newColumn("BACKUPS", Value.INT)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition idCond = conditionForColumn("ID", first, last);

        Collection<CacheGroupDescriptor> cacheGroups;

        if (idCond.isEquality()) {
            try {
                CacheGroupDescriptor cacheGrp = ctx.cache().cacheGroupDescriptors().get(idCond.valueForEquality().getInt());

                cacheGroups = cacheGrp == null ? Collections.emptySet() : Collections.singleton(cacheGrp);
            }
            catch (Exception ignore) {
                cacheGroups = Collections.emptySet();
            }
        }
        else
            cacheGroups = ctx.cache().cacheGroupDescriptors().values();

        return F.iterator(cacheGroups,
            grp -> {
                CacheConfiguration<?, ?> ccfg = grp.config();

                return createRow(
                    ses,
                    grp.groupId(),
                    grp.cacheOrGroupName(),
                    grp.sharedGroup(),
                    grp.caches() == null ? 0 : grp.caches().size(),
                    ccfg.getCacheMode(),
                    ccfg.getAtomicityMode(),
                    ccfg.getAffinity(),
                    ccfg.getAffinity() != null ? ccfg.getAffinity().partitions() : null,
                    nodeFilter(ccfg),
                    ccfg.getDataRegionName(),
                    ccfg.getTopologyValidator(),
                    ccfg.getPartitionLossPolicy(),
                    ccfg.getRebalanceMode(),
                    ccfg.getRebalanceDelay(),
                    ccfg.getRebalanceOrder(),
                    ccfg.getCacheMode() == CacheMode.REPLICATED ? null : ccfg.getBackups()
                );
            }, true);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.cache().cacheGroupDescriptors().size();
    }
}
