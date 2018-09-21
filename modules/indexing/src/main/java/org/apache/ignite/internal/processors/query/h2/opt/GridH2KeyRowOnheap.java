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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.h2.value.Value;

/**
 * Heap-based key-only row for remove operations.
 */
public class GridH2KeyRowOnheap extends GridH2Row {
    /** */
    private Value key;

    /**
     * @param row Row.
     * @param key Key.
     */
    public GridH2KeyRowOnheap(CacheDataRow row, Value key) {
        super(row);

        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        assert idx == 0 : idx;

        return key;
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        assert idx == 0 : idx;

        key = v;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        throw new UnsupportedOperationException();
    }
}
