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

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinAffinityIdentifier;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinGroup;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.h2.table.Column;
import org.h2.table.IndexColumn;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * Utility methods for partition extraction.
 */
public class PartitionExtractorUtils {

    public static PartitionJoinGroup joinGroupForTable(GridSqlAst from) {
        String alias = null;

        if (from instanceof GridSqlAlias) {
            alias = ((GridSqlAlias)from).alias();

            from = from.child();
        }

        if (from instanceof GridSqlTable) {
            GridSqlTable from0 = (GridSqlTable)from;

            GridH2Table tbl0 = from0.dataTable();

            if (tbl0 == null)
                return null;

            // Use identifier string because there might be two table with the same name but form different schemas.
            if (alias == null)
                alias = tbl0.identifierString();

            String cacheName = tbl0.cacheName();

            String affColName = null;

            for (Column col : tbl0.getColumns()) {
                // TODO: Wrong! We may have multiple affinity key oclumns here!
                if (isAffinityKeyColumn(col, tbl0)) {
                    affColName = col.getName();

                    break;
                }
            }

            PartitionJoinTable joinTbl = new PartitionJoinTable(alias, cacheName, affColName);

            CacheConfiguration ccfg = tbl0.cacheInfo().config();

            PartitionJoinAffinityIdentifier affIdentifier = affinityIdentifierForCache(ccfg);

            // TODO: Wrong.
            boolean replicated = affIdentifier != null && ccfg.getCacheMode() == CacheMode.REPLICATED;

            return new PartitionJoinGroup(affIdentifier, replicated).addTable(joinTbl);
        }

        return null;
    }

    /**
     * Prepare affinity identifier for cache.
     *
     * @param ccfg Cache configuration.
     * @return Affinity identifier.
     */
    private static PartitionJoinAffinityIdentifier affinityIdentifierForCache(CacheConfiguration ccfg) {
        return null;
    }

    /**
     * Check if the given column is affinity column.
     *
     * @param col Column.
     * @param tbl H2 Table.
     * @return is affinity key or not
     */
    public static boolean isAffinityKeyColumn(Column col, GridH2Table tbl) {
        int colId = col.getColumnId();

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc.isKeyColumn(colId))
            return true;

        IndexColumn affKeyCol = tbl.getAffinityKeyColumn();

        try {
            return
                affKeyCol != null &&
                colId >= DEFAULT_COLUMNS_COUNT &&
                desc.isColumnKeyProperty(colId - DEFAULT_COLUMNS_COUNT) &&
                colId == affKeyCol.column.getColumnId();
        }
        catch (IllegalStateException e) {
            return false;
        }
    }

    /**
     * Private constructor.
     */
    private PartitionExtractorUtils() {
        // No-op.
    }
}
