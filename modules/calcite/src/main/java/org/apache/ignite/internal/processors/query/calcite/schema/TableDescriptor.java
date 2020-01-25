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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;

/**
 *
 */
public interface TableDescriptor extends RelProtoDataType {
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
}
