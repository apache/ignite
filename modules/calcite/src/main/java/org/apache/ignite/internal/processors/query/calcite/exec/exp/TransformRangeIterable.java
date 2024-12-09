/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.util.Iterator;
import java.util.function.Function;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Row-transformable range iterable.
 */
public class TransformRangeIterable<FromRow, ToRow> implements RangeIterable<ToRow> {
    /** */
    private final RangeIterable<FromRow> delegate;

    /** */
    private final Function<FromRow, ToRow> rowTransformer;

    /** */
    public TransformRangeIterable(RangeIterable<FromRow> delegate, Function<FromRow, ToRow> rowTransformer) {
        this.delegate = delegate;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override public Iterator<RangeCondition<ToRow>> iterator() {
        return F.iterator(delegate.iterator(), c -> new TransformRangeCondition<>(c, rowTransformer), true);
    }

    /** {@inheritDoc} */
    @Override public boolean multiBounds() {
        return delegate.multiBounds();
    }

    /** */
    private static class TransformRangeCondition<FromRow, ToRow> implements RangeCondition<ToRow> {
        /** */
        private final RangeCondition<FromRow> delegate;

        /** */
        private final Function<FromRow, ToRow> rowTransformer;

        /** */
        public TransformRangeCondition(RangeCondition<FromRow> delegate, Function<FromRow, ToRow> rowTransformer) {
            this.delegate = delegate;
            this.rowTransformer = rowTransformer;
        }

        /** {@inheritDoc} */
        @Override public ToRow lower() {
            FromRow row = delegate.lower();

            return row == null ? null : rowTransformer.apply(row);
        }

        /** {@inheritDoc} */
        @Override public ToRow upper() {
            FromRow row = delegate.upper();

            return row == null ? null : rowTransformer.apply(row);
        }

        /** {@inheritDoc} */
        @Override public boolean lowerInclude() {
            return delegate.lowerInclude();
        }

        /** {@inheritDoc} */
        @Override public boolean upperInclude() {
            return delegate.upperInclude();
        }
    }
}
