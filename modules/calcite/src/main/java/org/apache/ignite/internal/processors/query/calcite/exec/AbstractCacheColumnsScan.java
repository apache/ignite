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

package org.apache.ignite.internal.processors.query.calcite.exec;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class AbstractCacheColumnsScan<Row> extends AbstractCacheScan<Row> {
    /** */
    protected final CacheTableDescriptor desc;

    /** */
    protected final RowFactory<Row> factory;

    /** */
    protected final RelDataType rowType;

    /** Participating columns. */
    protected final ImmutableBitSet requiredColumns;

    /** */
    AbstractCacheColumnsScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        int[] parts,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(ectx, desc.cacheContext(), parts);

        this.desc = desc;
        this.requiredColumns = requiredColumns;

        rowType = desc.rowType(ectx.getTypeFactory(), requiredColumns);
        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);
    }
}
