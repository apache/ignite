/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** */
public interface ColumnDescriptor {
    /** */
    boolean field();

    /** */
    boolean key();

    /** */
    boolean hasDefaultValue();

    /** */
    String name();

    /** */
    int fieldIndex();

    /** */
    RelDataType logicalType(IgniteTypeFactory f);

    /** */
    Class<?> javaType();

    /** */
    Object value(ExecutionContext ectx, GridCacheContext<?, ?> cctx, CacheDataRow src) throws IgniteCheckedException;

    /** */
    Object defaultValue();

    /** */
    void set(Object dst, Object val) throws IgniteCheckedException;
}
