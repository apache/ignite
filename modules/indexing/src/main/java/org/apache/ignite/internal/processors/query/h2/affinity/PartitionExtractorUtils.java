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

package org.apache.ignite.internal.processors.query.h2.affinity;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionAffinityFunctionType;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinAffinityDescriptor;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinGroup;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.h2.table.Column;

/**
 * Utility methods for partition extraction.
 */
public class PartitionExtractorUtils {

    /**
     * Prepare join group for a single table.
     *
     * @param from Table.
     * @return Join group.
     */
    public static PartitionJoinGroup joinGroupForTable(GridSqlAst from) {
        String alias = null;

        if (from instanceof GridSqlAlias) {
            alias = ((GridSqlAlias)from).alias();

            from = from.child();
        }

        if (from instanceof GridSqlTable) {
            // Normal table.
            GridSqlTable from0 = (GridSqlTable)from;

            GridH2Table tbl0 = from0.dataTable();

            if (tbl0 == null)
                // Unknown table type, e.g. temp table.
                return new PartitionJoinGroup(null).addTable(new PartitionJoinTable(alias));

            // Use identifier string because there might be two table with the same name but form different schemas.
            if (alias == null)
                alias = tbl0.identifierString();

            String cacheName = tbl0.cacheName();

            String affColName = null;
            String secondAffColName = null;

            for (Column col : tbl0.getColumns()) {
                if (tbl0.isColumnForPartitionPruningStrict(col)) {
                    if (affColName == null)
                        affColName = col.getName();
                    else {
                        secondAffColName = col.getName();

                        // Break as we cannot have more than two affinity key columns.
                        break;
                    }
                }
            }

            PartitionJoinTable joinTbl = new PartitionJoinTable(alias, cacheName, affColName, secondAffColName);

            PartitionJoinAffinityDescriptor affDesc = affinityDescriptorForCache(tbl0.cacheInfo().config());

            return new PartitionJoinGroup(affDesc).addTable(joinTbl);
        }
        else {
            // Subquery/union
            assert alias != null;

            return new PartitionJoinGroup(null).addTable(new PartitionJoinTable(alias));
        }
    }

    /**
     * Prepare affinity identifier for cache.
     *
     * @param ccfg Cache configuration.
     * @return Affinity identifier.
     */
    private static PartitionJoinAffinityDescriptor affinityDescriptorForCache(CacheConfiguration ccfg) {
        PartitionAffinityFunctionType aff = ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class) ?
            PartitionAffinityFunctionType.RENDEZVOUS : PartitionAffinityFunctionType.CUSTOM;

        return new PartitionJoinAffinityDescriptor(
            ccfg.getCacheMode(),
            aff,
            ccfg.getAffinity().partitions(),
            ccfg.getNodeFilter() != null
        );
    }

    /**
     * Private constructor.
     */
    private PartitionExtractorUtils() {
        // No-op.
    }
}
