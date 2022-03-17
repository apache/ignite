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
import java.util.List;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRowFactory;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

import static java.util.Objects.requireNonNull;

/**
 * Unsorted merge index.
 */
public class UnsortedReducer extends UnsortedBaseReducer {
    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public UnsortedReducer(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(SearchRow first, SearchRow last) {
        // This index is unsorted: have to ignore bounds.
        return new FetchingCursor(new Iterator<Row>() {
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
    private class FetchingCursor implements Cursor {
        /** */
        private Iterator<Row> stream;

        /** */
        private List<Row> rows;

        /** */
        private int cur;

        /**
         * @param stream Stream of all the rows from remote nodes.
         */
        FetchingCursor(Iterator<Row> stream) {
            assert stream != null;

            // Initially we will use all the fetched rows, after we will switch to the last block.
            rows = fetched;

            this.stream = stream;

            cur--; // Set current position before the first row.
        }

        /**
         * Fetch rows from the stream.
         */
        private void fetchRows() {
            // Take the current last block and set the position after last.
            rows = fetched.lastBlock();
            cur = rows.size();

            // Fetch stream.
            if (stream.hasNext()) {
                fetched.add(requireNonNull(stream.next()));

                // Evict block if we've fetched too many rows.
                if (fetched.size() == MAX_FETCH_SIZE) {
                    onBlockEvict(fetched.evictFirstBlock());

                    assert fetched.size() < MAX_FETCH_SIZE;
                }
            }

            if (cur == rows.size())
                cur = Integer.MAX_VALUE; // We were not able to fetch anything. Done.
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (cur == Integer.MAX_VALUE)
                return false;

            if (++cur == rows.size())
                fetchRows();

            return cur < Integer.MAX_VALUE;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return rows.get(cur);
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
