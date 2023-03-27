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

import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Base unsorted merge index.
 */
public abstract class UnsortedBaseReducer extends AbstractReducer {
    /** */
    protected final AtomicInteger activeSourcesCnt = new AtomicInteger(-1);

    /** */
    protected final PollableQueue<ReduceResultPage> queue = new PollableQueue<>();

    /** */
    protected Iterator<Value[]> iter = Collections.emptyIterator();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public UnsortedBaseReducer(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void setSources(Map<ClusterNode, BitSet> nodesToSegmentsCnt) {
        super.setSources(nodesToSegmentsCnt);

        int totalSegmentsCnt = nodesToSegmentsCnt.values().stream().mapToInt(BitSet::cardinality).sum();

        assert totalSegmentsCnt > 0 : totalSegmentsCnt;

        activeSourcesCnt.set(totalSegmentsCnt);
    }

    /** {@inheritDoc} */
    @Override public boolean fetchedAll() {
        int x = activeSourcesCnt.get();

        assert x >= 0 : x; // This method must not be called if the sources were not set.

        return x == 0 && queue.isEmpty();
    }

    /**
     * @param page Page.
     */
    @Override protected void addPage0(ReduceResultPage page) {
        assert page.rowsInPage() > 0 || page.isLast() || page.isFail();

        // Do not add empty page to avoid premature stream termination.
        if (page.rowsInPage() != 0 || page.isFail())
            queue.add(page);

        if (page.isLast()) {
            int x = activeSourcesCnt.decrementAndGet();

            assert x >= 0 : x;

            if (x == 0) // Always terminate with empty iterator.
                queue.add(createDummyLastPage(page));
        }
    }

    /** {@inheritDoc} */
    @Override protected Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first, @Nullable SearchRow last) {
        // This index is unsorted: have to ignore bounds.
        return new GridH2Cursor(fetched.iterator());
    }

    /** */
    private static class PollableQueue<X> extends LinkedBlockingQueue<X> implements AbstractReducer.Pollable<X> {
        // No-op.
    }
}
