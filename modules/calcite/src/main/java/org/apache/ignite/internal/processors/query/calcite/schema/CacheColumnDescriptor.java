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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/**
 * Column descriptor for cache tables.
 */
public interface CacheColumnDescriptor extends ColumnDescriptor {
    /**
     * Is column a field of composite object.
     *
     * @return {@code True} if column is a field of composite object, {@code false} if column is an entire object.
     */
    public boolean field();

    /**
     * Is column relate to key.
     *
     * @return {@code True} if column relate to key, {@code false} if column relate to value.
     */
    public boolean key();

    /**
     * Gets column value from CacheDataRow.
     */
    public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src)
        throws IgniteCheckedException;

    /**
     * Sets field of composite object value.
     */
    public void set(Object dst, Object val) throws IgniteCheckedException;
}
