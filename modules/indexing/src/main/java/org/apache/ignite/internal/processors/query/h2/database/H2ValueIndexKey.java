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
 *
 */

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * Index key wrapped over H2 Value.
 */
public class H2ValueIndexKey implements IndexKey {
    /** */
    private final CacheObjectValueContext coCtx;

    /** */
    private final Table table;

    /** */
    private final Value val;

    /** */
    public H2ValueIndexKey(CacheObjectValueContext coCtx, Table table, Value val) {
        this.coCtx = coCtx;
        this.table = table;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return val.getObject();
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.forCode(val.getType());
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) throws IgniteCheckedException {
        return compareValues(val, H2Utils.wrap(coCtx, o.key(), o.type().code()));
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    private int compareValues(Value v1, Value v2) {
        // Exploit convertion/comparison provided by H2.
        return Integer.signum(table.compareTypeSafe(v1, v2));
    }

    /** {@inheritDoc} */
    @Override public boolean isComparableTo(IndexKey k) {
        return true;
    }
}
