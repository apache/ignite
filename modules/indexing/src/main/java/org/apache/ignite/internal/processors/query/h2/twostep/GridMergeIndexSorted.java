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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase.bubbleUp;

/**
 * Sorted index.
 */
public final class GridMergeIndexSorted extends GridMergeIndex {
    /** */
    private final Comparator<RowStream> streamCmp = new Comparator<RowStream>() {
        @Override public int compare(RowStream o1, RowStream o2) {
            // Nulls at the beginning.
            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return compareRows(o1.get(), o2.get());
        }
    };

    /** */
    private Map<UUID,RowStream> streamsMap;

    /** */
    private RowStream[] streams;

    /**
     * @param ctx Kernal context.
     * @param tbl Table.
     * @param name Index name,
     * @param type Index type.
     * @param cols Columns.
     */
    public GridMergeIndexSorted(
        GridKernalContext ctx,
        GridMergeTable tbl,
        String name,
        IndexType type,
        IndexColumn[] cols
    ) {
        super(ctx, tbl, name, type, cols);
    }

    /** {@inheritDoc} */
    @Override public void setSources(Collection<ClusterNode> nodes) {
        super.setSources(nodes);

        streamsMap = U.newHashMap(nodes.size());
        streams = new RowStream[nodes.size()];

        int i = 0;

        for (ClusterNode node : nodes) {
            RowStream stream = new RowStream(node.id());

            streams[i] = stream;

            if (streamsMap.put(stream.src, stream) != null)
                throw new IllegalStateException();
        }
    }

    /** {@inheritDoc} */
    @Override protected void addPage0(GridResultPage page) {
        if (page.isLast() || page.isFail()) {
            // Finish all the streams.
            for (RowStream stream : streams)
                stream.addPage(page);
        }
        else {
            assert page.rowsInPage() > 0;

            UUID src = page.source();

            streamsMap.get(src).addPage(page);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkBounds(Row lastEvictedRow, SearchRow first, SearchRow last) {
        // If our last evicted fetched row was smaller than the given lower bound,
        // then we are ok. This is important for merge join to work.
        if (lastEvictedRow != null && first != null && compareRows(lastEvictedRow, first) < 0)
            return;

        super.checkBounds(lastEvictedRow, first, last);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findAllFetched(List<Row> fetched, SearchRow first, SearchRow last) {
        Iterator<Row> iter;

        if (fetched.isEmpty())
            iter = emptyIterator();
        else if (first == null && last == null)
            iter = fetched.iterator();
        else {
            int low = first == null ? 0 : binarySearchRow(fetched, first, firstRowCmp, false);

            if (low == fetched.size())
                iter = emptyIterator();
            else {
                int high = last == null ? fetched.size() : binarySearchRow(fetched, last, lastRowCmp, false);

                iter = fetched.subList(low, high).iterator();
            }
        }

        return new GridH2Cursor(iter);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        return new FetchingCursor(first, last, new MergeStreamIterator());
    }

    /**
     * Iterator merging multiple row streams.
     */
    private final class MergeStreamIterator implements Iterator<Row> {
        /** */
        private boolean first = true;

        /** */
        private int off;

        /** */
        private boolean hasNext;

        /**
         *
         */
        private void goFirst() {
            for (int i = 0; i < streams.length; i++) {
                if (!streams[i].next()) {
                    streams[i] = null;
                    off++; // Move left bound.
                }
            }

            if (off < streams.length)
                Arrays.sort(streams, streamCmp);

            first = false;
        }

        /**
         *
         */
        private void goNext() {
            if (streams[off].next())
                bubbleUp(streams, off, streamCmp);
            else
                streams[off++] = null; // Move left bound and nullify empty stream.
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (hasNext)
                return true;

            if (first)
                goFirst();
            else
                goNext();

            return hasNext = off < streams.length;
        }

        /** {@inheritDoc} */
        @Override public Row next() {
            if (!hasNext())
                throw new NoSuchElementException();

            hasNext = false;

            return streams[off].get();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Row stream.
     */
    private final class RowStream {
        /** */
        final UUID src;

        /** */
        final BlockingQueue<GridResultPage> queue = new ArrayBlockingQueue<>(8);

        /** */
        Iterator<Value[]> iter = emptyIterator();

        /** */
        Row cur;

        /**
         * @param src Source.
         */
        private RowStream(UUID src) {
            this.src = src;
        }

        /**
         * @param page Page.
         */
        private void addPage(GridResultPage page) {
            queue.offer(page);
        }

        /**
         * @return {@code true} If we successfully switched to the next row.
         */
        private boolean next() {
            cur = null;

            iter = pollNextIterator(queue, iter);

            if (!iter.hasNext())
                return false;

            cur = GridH2RowFactory.create(iter.next());

            return true;
        }

        /**
         * @return Current row.
         */
        private Row get() {
            assert cur != null;

            return cur;
        }
    }
}
