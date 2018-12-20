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
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinCondition;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinTable;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionTableModel;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.h2.table.Column;

/**
 * Utility methods for partition extraction.
 */
public class PartitionExtractorUtils {

    /**
     * Prepare single table.
     *
     * @param from Expression.
     * @param tblModel Table model.
     * @return Added table or {@code null} if table is exlcuded from the model.
     */
    public static PartitionJoinTable prepareTable(GridSqlAst from, PartitionTableModel tblModel) {
        String alias = null;

        if (from instanceof GridSqlAlias) {
            alias = ((GridSqlAlias)from).alias();

            from = from.child();
        }

        if (from instanceof GridSqlTable) {
            // Normal table.
            GridSqlTable from0 = (GridSqlTable)from;

            GridH2Table tbl0 = from0.dataTable();

            // Unknown table type, e.g. temp table.
            if (tbl0 == null) {
                tblModel.addExcludedTable(alias);

                return null;
            }

            // Use identifier string because there might be two table with the same name but form different schemas.
            // TODO: Be very careful here. Seems that alias should never be null here!
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

            PartitionJoinTable tbl = new PartitionJoinTable(alias, cacheName, affColName, secondAffColName);
            PartitionJoinAffinityDescriptor aff = affinityDescriptorForCache(tbl0.cacheInfo().config());

            tblModel.addTable(tbl, aff);

            return tbl;
        }
        else {
            // Subquery/union/view, etc.
            assert alias != null;

            tblModel.addExcludedTable(alias);

            return null;
        }
    }

    /**
     * Check whether this is a cross-join condition, i.e. 1=1.
     *
     * @param on Condition.
     * @return {@code True} if cross-join.
     */
    public static boolean isCrossJoinCondition(GridSqlAst on) {
        if (on instanceof GridSqlOperation) {
            GridSqlOperation on0 = (GridSqlOperation)on;

            if (on0.operationType() == GridSqlOperationType.EQUAL) {
                GridSqlConst leftConst = PartitionExtractor.unwrapConst(on0.child(0));
                GridSqlConst rightConst = PartitionExtractor.unwrapConst(on0.child(1));

                if (leftConst != null && rightConst != null) {
                    try {
                        int leftConstval = leftConst.value().getInt();
                        int rightConstVal = rightConst.value().getInt();

                        return leftConstval == rightConstVal;
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }
            }
        }

        return false;
    }

    /**
     * Try parsing condition as simple JOIN codition. Only equijoins are supported for now, so anything more complex
     * than "A.a = B.b" are not processed.
     *
     * @param on Initial AST.
     * @return Join condition or {@code null} if not simple equijoin.
     */
    public static PartitionJoinCondition parseJoinCondition(GridSqlElement on) {
        if (on instanceof GridSqlOperation) {
            GridSqlOperation on0 = (GridSqlOperation)on;

            if (on0.operationType() == GridSqlOperationType.EQUAL) {
                // Check for cross-join first.
                GridSqlConst leftConst = PartitionExtractor.unwrapConst(on0.child(0));
                GridSqlConst rightConst = PartitionExtractor.unwrapConst(on0.child(1));

                if (leftConst != null && rightConst != null) {
                    try {
                        int leftConstval = leftConst.value().getInt();
                        int rightConstVal = rightConst.value().getInt();

                        if (leftConstval == rightConstVal)
                            return PartitionJoinCondition.CROSS;
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }

                // This is not cross-join, neither normal join between columns.
                if (leftConst != null || rightConst != null)
                    return null;

                // Check for normal equi-join.
                GridSqlColumn left = PartitionExtractor.unwrapColumn(on0.child(0));
                GridSqlColumn right = PartitionExtractor.unwrapColumn(on0.child(1));

                if (left != null && right != null) {
                    String leftAlias = left.tableAlias();
                    String rightAlias = right.tableAlias();

                    String leftCol = left.columnName();
                    String rightCol = right.columnName();

                    return new PartitionJoinCondition(leftAlias, rightAlias, leftCol, rightCol);
                }
            }
        }

        return null;
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
