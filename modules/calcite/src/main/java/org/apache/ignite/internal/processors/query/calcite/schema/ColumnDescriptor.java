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
    Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src)
        throws IgniteCheckedException;

    /** */
    Object defaultValue();

    /** */
    void set(Object dst, Object val) throws IgniteCheckedException;
}
