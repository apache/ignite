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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.List;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface TableDescriptor extends RelProtoDataType, InitializerExpressionFactory {
    /**
     * @return Underlying cache context.
     */
    GridCacheContext<?,?> cacheContext();

    /**
     * @return Distribution.
     */
    IgniteDistribution distribution();

    /**
     * @return Collations.
     */
    List<RelCollation> collations();

    /**
     * Returns row type excluding effectively virtual fields.
     *
     * @param factory Type factory.
     * @return Row type for INSERT operation.
     */
    default RelDataType insertRowType(IgniteTypeFactory factory) {
        return apply(factory);
    }

    /**
     * Returns row type including effectively virtual fields.
     *
     * @param factory Type factory.
     * @return Row type for SELECT operation.
     */
    default RelDataType selectRowType(IgniteTypeFactory factory) {
        return apply(factory);
    }

    /**
     * Checks whether is possible to update a column with a given index.
     *
     * @param table Parent table.
     * @param iColumn Column index.
     * @return {@code True} if update operation is allowed for a column with a given index.
     */
    boolean isUpdateAllowed(RelOptTable table, int iColumn);

    /**
     * Checks whether a provided cache row belongs to described table.
     *
     * @param row Cache row.
     * @return {@code True} If a provided cache row matches a defined query type.
     */
    boolean match(CacheDataRow row);

    /**
     * Converts a cache row to relational node row.
     *
     * @param ectx Execution context.
     * @param row Cache row.
     * @return Relational node row.
     * @throws IgniteCheckedException If failed.
     */
    <T> T toRow(ExecutionContext ectx, CacheDataRow row) throws IgniteCheckedException;

    /**
     * Converts a relational node row to cache key-value tuple;
     *
     * @param ectx Execution context.
     * @param row Relational node row.
     * @param op Operation.
     * @param arg Operation specific argument.
     * @return Cache key-value tuple;
     * @throws IgniteCheckedException If failed.
     */
    <T> IgniteBiTuple<?,?> toTuple(ExecutionContext ectx, T row, TableModify.Operation op, @Nullable Object arg) throws IgniteCheckedException;
}
