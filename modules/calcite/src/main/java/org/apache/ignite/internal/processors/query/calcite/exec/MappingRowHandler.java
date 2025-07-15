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

import java.lang.reflect.Type;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Read only handler to process subset of columns.
 */
public class MappingRowHandler<Row> implements RowHandler<Row> {
    /** */
    private final RowHandler<Row> delegate;

    /** */
    private final int[] mapping;

    /** */
    public MappingRowHandler(RowHandler<Row> delegate, ImmutableBitSet requiredColumns) {
        this.delegate = delegate;
        mapping = requiredColumns.toArray();
    }

    /** {@inheritDoc} */
    @Override public Object get(int field, Row row) {
        return delegate.get(mapping[field], row);
    }

    /** {@inheritDoc} */
    @Override public int columnCount(Row row) {
        return mapping.length;
    }

    /** {@inheritDoc} */
    @Override public void set(int field, Row row, Object val) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Row concat(Row left, Row right) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RowFactory<Row> factory(Type... types) {
        throw new UnsupportedOperationException();
    }
}
