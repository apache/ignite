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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.h2.index.Cursor;
import org.h2.result.Row;

/**
 * Iterator that transparently and sequentially traverses a bunch of {@link GridMergeIndex} objects.
 */
class GridMergeIndexeIterator implements Iterator<List<?>> {
    /** Reduce query executor. */
    private final GridReduceQueryExecutor rdcExec;

    /** Participating nodes. */
    private final Collection<ClusterNode> nodes;

    /** Query run. */
    private final ReduceQueryRun run;

    /** Query request ID. */
    private final long qryReqId;

    /** Distributed joins. */
    private final boolean distributedJoins;

    /** Iterator over indexes. */
    private final Iterator<GridMergeIndex> idxIter;

    /** Current cursor. */
    private Cursor cursor;

    /** Next row to return. */
    private List<Object> next;

    /** Whether remote resources were released. */
    private boolean released;

    /**
     * Constructor.
     *
     * @param rdcExec Reduce query executor.
     * @param nodes Participating nodes.
     * @param run Query run.
     * @param qryReqId Query request ID.
     * @param distributedJoins Distributed joins.
     * @throws IgniteCheckedException if failed.
     */
    GridMergeIndexeIterator(GridReduceQueryExecutor rdcExec, Collection<ClusterNode> nodes, ReduceQueryRun run,
        long qryReqId, boolean distributedJoins)
        throws IgniteCheckedException {
        this.rdcExec = rdcExec;
        this.nodes = nodes;
        this.run = run;
        this.qryReqId = qryReqId;
        this.distributedJoins = distributedJoins;

        this.idxIter = run.indexes().iterator();

        advance();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override public List<?> next() {
        List<Object> res = next;

        if (res == null)
            throw new NoSuchElementException();

        advance();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
    }

    /**
     * Advance iterator.
     */
    private void advance() {
        try {
            while (true) {
                // Get next index cursor to iterate over.
                if (cursor == null && idxIter.hasNext())
                    cursor = idxIter.next().findInStream(null, null);
                else {
                    next = null;

                    break;
                }

                assert cursor != null;

                if (cursor.next()) {
                    Row row = cursor.get();

                    int cols = row.getColumnCount();

                    List<Object> res = new ArrayList<>(cols);

                    for (int c = 0; c < cols; c++)
                        res.add(row.getValue(c).getObject());

                    next = res;

                    break;
                }
                else
                    cursor = null;
            }

            if (next == null)
                releaseIfNeeded();
        }
        catch (Exception e) {
            releaseIfNeeded();

            throw e;
        }
    }

    /**
     * Close routine.
     */
    private void releaseIfNeeded() {
        if (!released) {
            try {
                rdcExec.releaseRemoteResources(nodes, run, qryReqId, distributedJoins);
            }
            finally {
                released = true;
            }
        }
    }
}
