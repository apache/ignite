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

import org.apache.calcite.rel.core.TableModify;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@SuppressWarnings("rawtypes")
public interface CacheTableDescriptor extends TableDescriptor<CacheDataRow> {
    /**
     * @return Underlying cache context info.
     */
    GridCacheContextInfo cacheInfo();

    /**
     * @return Underlying cache context.
     */
    GridCacheContext cacheContext();

    /**
     * Checks whether a provided cache row belongs to described table.
     *
     * @param row Cache row.
     * @return {@code True} If a provided cache row matches a defined query type.
     */
    boolean match(CacheDataRow row);

    /**
     * Converts a relational node row to cache key-value tuple with table operation.
     *
     * @param ectx Execution context.
     * @param row Relational node row.
     * @param op Operation.
     * @param arg Operation specific argument.
     * @return Cache key-value tuple;
     * @throws IgniteCheckedException If failed.
     */
    <Row> ModifyTuple toTuple(ExecutionContext<Row> ectx, Row row, TableModify.Operation op, @Nullable Object arg)
        throws IgniteCheckedException;

    /**
     * @return Type descriptor.
     */
    GridQueryTypeDescriptor typeDescription();
}
