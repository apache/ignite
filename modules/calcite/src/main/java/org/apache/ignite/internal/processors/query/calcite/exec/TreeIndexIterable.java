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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeCondition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Tree index iterable.
 */
public class TreeIndexIterable<Row> implements Iterable<Row> {
    /** */
    private final TreeIndex<Row> idx;

    /** Index scan bounds. */
    private final RangeIterable<Row> ranges;

    /**
     * @param idx Tree index.
     * @param ranges Index scan bounds.
     */
    TreeIndexIterable(TreeIndex<Row> idx, RangeIterable<Row> ranges) {
        this.idx = idx;
        this.ranges = ranges;
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<Row> iterator() {
        if (ranges == null)
            return new CursorIteratorImpl(idx.find(null, null, true, true));

        IgniteClosure<RangeCondition<Row>, CursorIteratorImpl> clo = range -> new CursorIteratorImpl(
                idx.find(range.lower(), range.upper(), range.lowerInclude(), range.upperInclude()));

        if (!ranges.multiBounds()) {
            Iterator<RangeCondition<Row>> it = ranges.iterator();

            if (it.hasNext())
                return clo.apply(it.next());
            else
                return Collections.emptyIterator();
        }

        return F.flat(F.iterator(ranges, clo, true));
    }

    /** */
    private class CursorIteratorImpl extends GridIteratorAdapter<Row> {
        /** */
        private final GridCursor<Row> cursor;

        /** Next element. */
        private Row next;

        /** */
        private CursorIteratorImpl(@NotNull GridCursor<Row> cursor) {
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            advance();

            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Row nextX() throws IgniteCheckedException {
            advance();

            if (next == null)
                throw new NoSuchElementException();

            Row res = next;

            next = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            throw new UnsupportedOperationException("Remove is not supported.");
        }

        /** */
        private void advance() throws IgniteCheckedException {
            assert cursor != null;

            if (next != null)
                return;

            while (next == null && cursor.next())
                next = cursor.get();
        }
    }
}
