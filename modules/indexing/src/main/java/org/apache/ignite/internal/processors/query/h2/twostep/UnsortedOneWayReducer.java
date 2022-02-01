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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Iterator;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRowFactory;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import static java.util.Objects.requireNonNull;

/**
 * Unsorted one-way merge index.
 */
public class UnsortedOneWayReducer extends UnsortedBaseReducer {
    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public UnsortedOneWayReducer(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param ctx Context.
     * @return Dummy index instance.
     */
    public static UnsortedOneWayReducer createDummy(GridKernalContext ctx) {
        return new UnsortedOneWayReducer(ctx);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(SearchRow first, SearchRow last) {
        assert first == null && last == null : "Invalid usage dummy reducer: [first=" + first + ", last=" + last + ']';

        return new OneWayFetchingCursor(new Iterator<Row>() {
            @Override public boolean hasNext() {
                iter = pollNextIterator(queue, iter);

                return iter.hasNext();
            }

            @Override public Row next() {
                return H2PlainRowFactory.create(iter.next());
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    /**
     * Fetching cursor.
     */
    private class OneWayFetchingCursor implements Cursor {
        /** */
        private Iterator<Row> stream;

        /** */
        private Row cur;

        /**
         * @param stream Stream of all the rows from remote nodes.
         */
        OneWayFetchingCursor(Iterator<Row> stream) {
            assert stream != null;

            this.stream = stream;
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (!stream.hasNext())
                return false;

            cur = requireNonNull(stream.next());

            return true;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return cur;
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            // Should never be called.
            throw DbException.getUnsupportedException("previous");
        }
    }
}
