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

package org.apache.ignite.internal.processors.query.h2.opt;


import org.apache.ignite.IgniteCheckedException;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Onheap row.
 */
public class GridH2KeyValueRowOnheap extends GridH2AbstractKeyValueRow {
    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param key Key.
     * @param keyType Key type.
     * @param val Value.
     * @param valType Value type.
     * @param expirationTime Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2KeyValueRowOnheap(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val, int valType,
        long expirationTime) throws IgniteCheckedException {
        super(desc, key, keyType, val, valType, expirationTime);
    }

    /** {@inheritDoc} */
    @Override protected void cache() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected Value getOffheapValue(int col) {
        return null;
    }
}